import config from './config.js';
import { getNowDatetimeString } from './libs.js';
import oracledb from 'oracledb';
import * as remainBagDB from './packing/oraclePackingRemain.js';
import * as VisionTagsAPI from './VisionTagsAPI.js';
import * as Mailer from './mailer.js';

//定時抓已結束的排程計算X-RM管制統計值，TODO:生產結束時，檢查是否仍有殘包寄信
export async function scheduleMonitor(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    const tagsType = ['ec', 'rpm'];
    const filter = true; //要不要將Tag的異常值濾掉
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT SCH.LINE, SCH.SEQ, SCH.LOT_NO, SCH.PRD_PC, SCH.ACT_STR_TIME, SCH.ACT_END_TIME, SCH.SCH_SEQ, STAT.SEQUENCE
            FROM PRO_SCHEDULE SCH LEFT JOIN PBTC_IOT_EXTR_STATISTICS STAT
                ON SCH.LINE = STAT.LINE
                AND SCH.SEQ = STAT.SEQUENCE
                AND SCH.COMPANY = STAT.COMPANY
                AND SCH.FIRM = STAT.FIRM
            WHERE SCH.ACT_STR_TIME > TO_DATE('20221201', 'YYYYMMDD') --從20221201回補
            AND SCH.ACT_END_TIME IS NOT NULL
            AND SCH.LINE != 'S' --S線尚未建立
            AND STAT.SEQUENCE IS NULL
            AND SCH.COMPANY = :COMPANY
            AND SCH.FIRM = :FIRM `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        //抓每一個排程每分鐘的Tag值做統計計算
        for (let schedule of scheduleResult.rows) {
            let mongoResult = await VisionTagsAPI.getStatisticsArray(schedule.LINE, schedule.ACT_STR_TIME, schedule.ACT_END_TIME, filter, user);
            if (!mongoResult.error) {
                for (const tagType of tagsType) {
                    //最多約莫15000個
                    console.log(`scheduleMonitor: ${schedule.LINE}-${schedule.SEQ}, ${tagType}`);

                    //抓押出機轉速與負載的押出製造上下限管制
                    let tolerance = 20;
                    let base = ('rpm' === tagType) ? 290 : 670;
                    sql = `
                        SELECT TOLERANCE, BASE
                        FROM PBTC_IOT_EXTRUSION_STD
                        WHERE PRODUCT_NO = :PRODUCT_NO
                        AND LINE = :LINE
                        AND VER = :VER
                        AND SEQUENCE = :SEQUENCE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM `;
                    params = {
                        PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PRD_PC },
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.LINE },
                        VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.SCH_SEQ },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: ('rpm' === tagType ? 16 : 17) },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    let stdArray = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                    if (stdArray.rows.length) {
                        tolerance = stdArray.rows[0].TOLERANCE;
                        base = stdArray.rows[0].BASE;
                    }
                    let statisticsResult = arrayStatistics(mongoResult[tagType], tolerance, base);

                    sql = `
                        INSERT INTO PBTC_IOT_EXTR_STATISTICS (
                            LINE, SEQUENCE, LOT_NO, PRD_PC, ACT_STR_TIME, ACT_END_TIME, TAG_TYPE, 
                            AVERAGE, RM, NUM, UCL, LCL, STD, RMSTD, USL, LSL, CPK, 
                            COMPANY, FIRM )
                        VALUES (
                            :LINE, :SEQUENCE, :LOT_NO, :PRD_PC, :ACT_STR_TIME, :ACT_END_TIME, :TAG_TYPE, 
                            :AVERAGE, :RM, :NUM, :UCL, :LCL, :STD, :RMSTD, :USL, :LSL, :CPK,
                            :COMPANY, :FIRM ) `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.LINE },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: schedule.SEQ },
                        LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.LOT_NO },
                        PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PRD_PC },
                        ACT_STR_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: schedule.ACT_STR_TIME },
                        ACT_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: schedule.ACT_END_TIME },
                        TAG_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: tagType },
                        AVERAGE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.avg || 0 },
                        RM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.Rm || 0 },
                        NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.count },
                        UCL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.UCL || 0 },
                        LCL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.LCL || 0 },
                        STD: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.std || 0 },
                        RMSTD: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.Rmstd || 0 },
                        USL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: base + tolerance },
                        LSL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: base - tolerance },
                        CPK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: statisticsResult.Cpk || 0 },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    await conn.execute(sql, params, { autoCommit: true });
                }
            }

            //檢查與該排程相符之成品簡碼是否已經將剩餘的殘包用完
            const remainResult = await remainBagDB.getProductLono(user, schedule.PRD_PC);
            if (remainResult.res.length) {
                const rowData = remainResult.res[0];
                //批號不同才發信
                if (rowData.LOT_NO !== schedule.LOT_NO) {
                    await Mailer.alertNotUsingRemain(user, schedule.LINE, schedule.SEQ, schedule.PRD_PC, schedule.ACT_END_TIME, rowData.LONO, rowData.WEIGHT, rowData.LOT_NO);
                }
            }
        }

        //將結束的所有排程統一寄信，通知給相關人員
        if (scheduleResult.rows.length) {
            await Mailer.scheduleMonitorAlert(scheduleResult.rows, user);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'scheduleMonitor Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

/**
 * 計算X-RM管制圖各項統計值，FIXME:Cp, Cpk, Ppk怪怪的再調整
 * @param {Array} tagsArray
 * @param {Number} tolerance //公差
 * @param {Number} base //基準值
 */
const arrayStatistics = (tagsArray, tolerance, base) => {
    let obj = {
        count: tagsArray.length, //數量
        avg: 0, //平均值
        std: 0, //標準差
        Rm: 0, //Rm平均值
        Rmstd: 0, //Rm標準差
        UCL: 0, //上限值
        LCL: 0, //下限值
        USL: base + tolerance, //管制上限
        LSL: base - tolerance, //管制下限
        Cpk: 0,
    };

    obj.avg = tagsArray.reduce((a, c) => a + c, 0) / obj.count;

    let lastValue = 0;
    tagsArray.forEach((value, index) => {
        obj.Rm += index ? Math.abs(value - lastValue) : 0;
        obj.std += Math.pow(value - obj.avg, 2);
        lastValue = value;
    });
    obj.std = Math.sqrt(obj.std / (obj.count - 1));
    obj.Rm /= (obj.count - 1);
    obj.Rmstd = obj.Rm / 1.128;
    obj.UCL = obj.avg + (2.66 * obj.Rm);
    obj.LCL = obj.avg - (2.66 * obj.Rm);
    //FIXME:因押出製造標準幾乎都尚未建立，故先以此"不太正確"的算法計算Cpk
    obj.Cpk = 1 + Math.abs(Math.min((obj.UCL - obj.avg) / (3 * obj.std), (obj.avg - obj.LCL) / (3 * obj.std)));
    /*
    obj.Cpk = Math.min((obj.USL - obj.avg) / (3 * obj.std), (obj.avg - obj.LSL) / (3 * obj.std));
    let t = obj.USL - obj.LSL;
    let u = (obj.USL + obj.LSL) / 2;
    obj.Cpk = (1 - Math.abs((obj.avg - u) * 2 / t)) * (t / (6 * obj.std));
    */
    return obj;
};
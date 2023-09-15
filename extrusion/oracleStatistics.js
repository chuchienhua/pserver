import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import moment from 'moment';
import oracledb from 'oracledb';
import * as storageDB from './oracleStorage.js';
import * as VisionTagsAPI from '../VisionTagsAPI.js';
import { getWorkShiftTime } from './oracleForm.js';

//定時抓已結束的排程計算X-RM管制統計值
export async function getExtruderStatistics(line, sequence, productNo, startDate, endDate, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT *
            FROM  PBTC_IOT_EXTR_STATISTICS
            WHERE LINE = :LINE
            ${('*' === sequence) ? '' : `AND SEQUENCE = '${sequence}'`}
            ${('*' === productNo) ? '' : `AND PRD_PC = '${productNo}'`}
            AND ACT_END_TIME >= TO_DATE(:START_DATE, 'YYYYMMDD')
            AND ACT_END_TIME <= TO_DATE(:END_DATE, 'YYYYMMDD')
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY LINE, SEQUENCE `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getExtruderStatistics', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得時間區間內的時間項目區間
export async function getShutdown(startDate, endDate, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT 
                REPORT_DATE,
                SUM(STOP_1) AS STOP_1,
                SUM(STOP_2) AS STOP_2,
                SUM(STOP_3) AS STOP_3,
                SUM(STOP_4) AS STOP_4,
                SUM(STOP_5) AS STOP_5,
                SUM(STOP_6) AS STOP_6,
                SUM(STOP_7) AS STOP_7,
                SUM(STOP_TIME) AS STOP_TIME
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE >= :START_DATE
            AND REPORT_DATE < :END_DATE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY REPORT_DATE
            ORDER BY REPORT_DATE `;
        params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getShutdown', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//班別查詢用電量
export async function getDailyPowerConsumption(date, workShift, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //根據班別找出對應的時間
        const workShiftTime = getWorkShiftTime(date, workShift);

        //查詢此班有在生產中的排程時間
        const sql = `
            SELECT LINE, SEQ, PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME, WT_PER_HR
            FROM PRO_SCHEDULE
            WHERE ( 
                ( ACT_END_TIME > :WORK_SHIFT_END_TIME )
                OR ( ACT_END_TIME IS NULL ) 
                OR ( ACT_END_TIME > :WORK_SHIFT_START_TIME AND ACT_END_TIME <= :WORK_SHIFT_END_TIME ) )
            AND ACT_STR_TIME < :WORK_SHIFT_END_TIME
            AND ACT_STR_TIME > TO_DATE('20230801', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束?
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY LINE, SEQ `;
        const params = {
            WORK_SHIFT_START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.startTime) },
            WORK_SHIFT_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.endTime) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        for (const schedule of scheduleResult.rows) {
            const scheduleStartTime = (schedule.ACT_STR_TIME < workShiftTime.startTime) ? workShiftTime.startTime : schedule.ACT_STR_TIME;
            const scheduleEndTime = (!schedule.ACT_END_TIME || schedule.ACT_END_TIME > workShiftTime.endTime) ? workShiftTime.endTime : schedule.ACT_END_TIME;

            //該工令的實際產量
            const payResult = await storageDB.getInvtPay('time', null, null, scheduleStartTime, scheduleEndTime, schedule.LOT_NO, user);
            schedule['PAY_WEIGHT'] = payResult.res.length ? payResult.res[0].FEED_STORAGE : 0;

            //取得用電量
            const powerResult = await VisionTagsAPI.getPowerConsumption(schedule.LINE, scheduleStartTime, scheduleEndTime, user);
            schedule['POWER_CONSUMPTION'] = powerResult.res['MAX_POWER'] - powerResult.res['MIN_POWER'];

            schedule['DATE'] = date;
            schedule['WORK_SHIFT'] = workShift;

            obj.res.push(schedule);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getDailyPowerConsumption', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//工令區間用電量查詢
export async function getSchedulePowerConsumption(line, seqStart, seqEnd, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT S0.LINE, S0.SEQ, S0.PRD_PC, S0.LOT_NO, S0.ACT_STR_TIME, S0.ACT_END_TIME, S1.VISION_START_TIME, S1.VISION_END_TIME
            FROM PRO_SCHEDULE S0 LEFT JOIN PBTC_IOT_SCHEDULE S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.LINE = S1.LINE
                AND S0.SEQ = S1.SEQUENCE
            WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.LINE = :LINE
            AND S0.SEQ BETWEEN :SEQ_START AND :SEQ_END
            ORDER BY S0.SEQ `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(seqStart) },
            SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(seqEnd) },
        };
        const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        for (const schedule of scheduleResult.rows) {
            //該工令料頭、前料使用量
            const reworkResult = await storageDB.getOrderReworkWeight(schedule.LINE, schedule.SEQ, null, null, user);
            schedule['SCRAP_WEIGHT'] = reworkResult.scrap;
            schedule['HEAD_WEIGHT'] = reworkResult.head;

            //該工令包裝量
            const packResult = await storageDB.getOrderPacking(schedule.LINE, schedule.SEQ, null, null, user);
            if (packResult.rows.length) {
                schedule['PACK_WEIGHT'] = packResult.rows[0].TOTAL_WEIGHT;
                const remainBagResult = await storageDB.getOrderRemainBag(schedule.LINE, schedule.SEQ, user);
                if (remainBagResult.rows.length) {
                    schedule['REMAIN_BAG_WEIGHT'] = remainBagResult.rows.length ? remainBagResult.rows[0].REMAIN_BAG_WEIGHT : 0;
                }
            }

            //重工/改番入料量
            const reworkFeedResult = await storageDB.getLotNoReworkFeed(schedule.LOT_NO, user);
            schedule['REWORK_FEED'] = reworkFeedResult.res;

            //取得用電量
            const powerResult = await VisionTagsAPI.getPowerConsumption(schedule.LINE, schedule.ACT_STR_TIME, schedule.ACT_END_TIME, user);
            schedule['POWER_CONSUMPTION'] = powerResult.res['MAX_POWER'] - powerResult.res['MIN_POWER'];

            obj.res.push(schedule);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSchedulePowerConsumption', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得日期區間的停俥時間用電量
export async function getStopPowerConsumption(startTime, endTime, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //先取得這個時間區間內生產中的排程
        const sql = `
            SELECT LINE, SEQ, PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME, WT_PER_HR
            FROM PRO_SCHEDULE
            WHERE ( 
                ( ACT_END_TIME > :END_TIME )
                OR ( ACT_END_TIME IS NULL ) 
                OR ( ACT_END_TIME > :START_TIME AND ACT_END_TIME <= :END_TIME ) )
            AND ACT_STR_TIME < :END_TIME
            AND ACT_STR_TIME > TO_DATE('20230801', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束?
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY LINE, SEQ `;
        const params = {
            START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: moment(startTime, 'YYYY-MM-DD').toDate() },
            END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: moment(endTime, 'YYYY-MM-DD').toDate() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        //停俥區間為上一筆結束到下一筆開始之間
        let lastOrderEndTime;
        let lastLine;
        let lastSequence;
        for (const schedule of scheduleResult.rows) {
            if (lastOrderEndTime && lastLine === schedule.LINE) {
                //取得用電量
                const powerResult = await VisionTagsAPI.getPowerConsumption(schedule.LINE, lastOrderEndTime, schedule.ACT_STR_TIME, user);
                const powerConsumption = powerResult.res['MAX_POWER'] - powerResult.res['MIN_POWER'];

                obj.res.push({
                    LINE: schedule.LINE,
                    SEQ_END: lastSequence,
                    SEQ_START: schedule.SEQ,
                    STOP_START_TIME: lastOrderEndTime,
                    STOP_END_TIME: schedule.ACT_STR_TIME,
                    CONSUMPTION: powerConsumption,
                });
            }

            lastOrderEndTime = schedule.ACT_END_TIME;
            lastLine = schedule.LINE;
            lastSequence = schedule.SEQ;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getStopPowerConsumption', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
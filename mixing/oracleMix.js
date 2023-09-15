import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import oracledb from 'oracledb';
import * as PrinterAPI from '../printLabel.js';
import * as Mailer from '../mailer.js';

/* 拌粉作業 */
//取得原料領用排程表
export async function getMixingSchedule(startDate, endDate, line, sequence, productNo, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT 
                MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, 
                SEMI_NO, PRD_PC, MIXER, OPERATOR, NOTE,
                MIN(BATCH_SEQ) AS BATCH_START, 
                MAX(BATCH_SEQ) AS BATCH_END, 
                SUM(BATCH_WEIGHT) AS TOTAL_WEIGHT,
                SUM(PICK_WEIGHT) AS PICK_WEIGHT,
                MAX(LABEL_STATUS) AS LABEL_STATUS
            FROM PBTC_IOT_MIX
            WHERE TO_CHAR( MIX_DATE, 'YYYYMMDD' ) >= :MIX_DATE_START
            AND TO_CHAR( MIX_DATE, 'YYYYMMDD' ) <= :MIX_DATE_END
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )`;
        let params = {
            MIX_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            MIX_DATE_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('*' !== line) {
            sql += 'AND LINE = :LINE ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
        }
        if ('*' !== sequence) {
            sql += 'AND SEQUENCE = :SEQUENCE ';
            params['SEQUENCE'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) };
        }
        if ('*' !== productNo) {
            sql += 'AND PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo };
        }

        sql += `
            GROUP BY MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO, PRD_PC, MIXER, OPERATOR, LABEL_STATUS, NOTE
            ORDER BY MIX_DATE, 
            CASE
                WHEN WORK_SHIFT = '早' THEN 1
                WHEN WORK_SHIFT = '中' THEN 2
                WHEN WORK_SHIFT = '晚' THEN 3
                ELSE 4
            END, 
            LINE, SEQUENCE `;
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMixingSchedule Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//建立一筆拌粉排程
export async function createMixingSchedule(date, workShift, line, sequence, semiNo, batchStart, batchEnd, operator, note, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let batchRatio = 1; //正常狀況下，批數都是整數
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查生產排程是否存在
        sql = `
            SELECT LINE, SEQ, PRD_PC, BATCH_NM, PRO_WT
            FROM PRO_SCHEDULE
            WHERE 1 = 1
            AND PRO_SCHEDULE.LINE = :LINE
            AND PRO_SCHEDULE.SEQ = :SEQUENCE
            AND PRO_SCHEDULE.COMPANY = :COMPANY
            AND PRO_SCHEDULE.FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: sequence.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!result.rows.length) {
            obj.res = '排程不存在';
            obj.error = true;
            return obj;
        } else if ((batchStart !== batchEnd) && !Number.isInteger(batchStart)) {
            obj.res = '若設定小數點，則起始批數與結束批數需相同';
            obj.error = true;
            return obj;
        }

        //如果不為小數點批數排程，或此工令有尚未列印，則不可再建立
        if (Number.isInteger(batchEnd)) {
            sql = `
                SELECT PRD_PC
                FROM PBTC_IOT_MIX
                WHERE MIX_DATE = :MIX_DATE
                AND WORK_SHIFT = :WORK_SHIFT
                AND LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND SEMI_NO = :SEMI_NO
                AND LABEL_STATUS = 0
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                obj.res = '班別已建立過此排程，請直接更新';
                obj.error = true;
                return obj;
            }

            //找出起始批數須為多少
            sql = `
                SELECT
                    MAX(BATCH_SEQ) AS BATCH_END
                FROM PBTC_IOT_MIX
                WHERE MIX_DATE = :MIX_DATE
                AND WORK_SHIFT = :WORK_SHIFT
                AND LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND SEMI_NO = :SEMI_NO
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                GROUP BY MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                if (result.rows[0].BATCH_END + 1 !== batchStart) {
                    obj.res = `起始批數必須設為${result.rows[0].BATCH_END + 1}`;
                    obj.error = true;
                    return obj;
                }
            }
        }

        for (let i = batchStart; i <= batchEnd; i++) {
            if ((batchStart === batchEnd) && !Number.isInteger(batchStart)) {
                batchRatio = Math.round((batchStart % 1) * 100) / 100; //取小數再整理至小數點第二位
            }

            sql = `
                INSERT INTO PBTC_IOT_MIX ( 
                    UKEY, SEMI_NO, MATERIAL, RATIO, BATCH_SEQ,
                    MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, 
                    PRD_PC, BATCH_WEIGHT, MIXER, OPERATOR, NOTE,
                    COMPANY, FIRM )
                SELECT
                    PRO_SCHEDULE.UKEY, :SEMI_NO, PBTC_IOT_RECIPE.MATERIAL, PBTC_IOT_RECIPE.RATIO, :BATCH_SEQ,
                    :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE,
                    PRO_SCHEDULE.PRD_PC,
                    PRO_SCHEDULE.BATCH_WT * PBTC_IOT_RECIPE.RATIO * 0.01 * :BATCH_RATIO,
                    PBTC_IOT_RECIPE.MIXER, :OPERATOR, :NOTE,
                    :COMPANY, :FIRM
                FROM PRO_SCHEDULE, PBTC_IOT_RECIPE
                WHERE PRO_SCHEDULE.LINE = :LINE
                AND PRO_SCHEDULE.SEQ = :SEQUENCE
                AND PRO_SCHEDULE.COMPANY = :COMPANY
                AND PRO_SCHEDULE.FIRM = :FIRM
                AND PRO_SCHEDULE.PRD_PC = PBTC_IOT_RECIPE.PRODUCT_NO
                AND PBTC_IOT_RECIPE.LINE = :LINE
                AND PBTC_IOT_RECIPE.SEMI_NO = :SEMI_NO
                AND PBTC_IOT_RECIPE.CREATE_TIME = (
                    SELECT MAX( CREATE_TIME ) 
                    FROM PBTC_IOT_RECIPE
                    WHERE PRODUCT_NO = PRO_SCHEDULE.PRD_PC
                    AND VER = PRO_SCHEDULE.SCH_SEQ
                    AND LINE = :LINE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM )`;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                BATCH_RATIO: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchRatio) },
                BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(i) },
                OPERATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: operator.toString() },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (note) ? note : '' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { autoCommit: false });
            if (!result.rowsAffected) {
                obj.res = '未找到相符的配方，請確認生產排程配方別與線別是否已建立';
                obj.error = true;
                return obj;
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'createMixingSchedule Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新原料領用排程表
export async function updateMixingSchedule(date, workShift, line, sequence, semiNo, mixer, batchStart, batchEnd, operator, note, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查BATCH_SEQ是否已存在於其日的排程下
        sql = `
            SELECT UKEY
            FROM PBTC_IOT_MIX
            WHERE ( MIX_DATE != :MIX_DATE OR WORK_SHIFT != :WORK_SHIFT )
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND ( BATCH_SEQ >= :BATCH_START AND BATCH_SEQ <= :BATCH_END ) `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            BATCH_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchStart) },
            BATCH_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchEnd) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.res = '拌粉排程已被設定過，請再確認';
            obj.error = true;
            return obj;
        }

        //檢查拌粉排程的PrimaryKey是否已存在，不存在則直接Insert，存在則Update攪拌機、批數、操作人員
        for (let i = batchStart; i <= batchEnd; i++) {
            sql = `
                BEGIN
                    INSERT INTO PBTC_IOT_MIX ( 
                        UKEY, SEMI_NO, MATERIAL, RATIO, BATCH_SEQ,
                        MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, 
                        PRD_PC, BATCH_WEIGHT, MIXER, OPERATOR, NOTE,
                        COMPANY, FIRM )
                    SELECT
                        PRO_SCHEDULE.UKEY, PBTC_IOT_RECIPE.SEMI_NO, PBTC_IOT_RECIPE.MATERIAL, PBTC_IOT_RECIPE.RATIO, :BATCH_SEQ,
                        :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE,
                        PRO_SCHEDULE.PRD_PC,
                        PRO_SCHEDULE.BATCH_WT * PBTC_IOT_RECIPE.RATIO * 0.01,
                        :MIXER, :OPERATOR, :NOTE,
                        :COMPANY, :FIRM
                    FROM PRO_SCHEDULE, PBTC_IOT_RECIPE
                    WHERE PRO_SCHEDULE.LINE = :LINE
                    AND PRO_SCHEDULE.SEQ = :SEQUENCE
                    AND PRO_SCHEDULE.PRD_PC = PBTC_IOT_RECIPE.PRODUCT_NO
                    AND PRO_SCHEDULE.COMPANY = :COMPANY
                    AND PRO_SCHEDULE.FIRM = :FIRM
                    AND PBTC_IOT_RECIPE.LINE = :LINE
                    AND PBTC_IOT_RECIPE.SEMI_NO = :SEMI_NO
                    AND PBTC_IOT_RECIPE.CREATE_TIME = ( 
                        SELECT MAX( CREATE_TIME ) 
                        FROM PBTC_IOT_RECIPE
                        WHERE PRODUCT_NO = PRO_SCHEDULE.PRD_PC
                        AND VER = PRO_SCHEDULE.SCH_SEQ
                        AND LINE = :LINE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM );
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE PBTC_IOT_MIX
                        SET MIXER = :MIXER,
                            OPERATOR = :OPERATOR,
                            NOTE = :NOTE
                        WHERE LINE = :LINE
                        AND SEQUENCE = :SEQUENCE
                        AND BATCH_SEQ = :BATCH_SEQ
                        AND SEMI_NO = :SEMI_NO
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM;
                END; `;
            params = {
                MIXER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: mixer.toString() },
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(i) },
                OPERATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: operator.toString() },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (note) ? note : '' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }

        //移除空白的拌粉排程(BATCH可能UPDATE變少)
        sql = `
            DELETE PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE 
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND ( BATCH_SEQ < :BATCH_START OR BATCH_SEQ > :BATCH_END ) `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            BATCH_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchStart) },
            BATCH_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchEnd) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateMixingSchedule Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//刪除原料領用排程表
export async function removeMixingSchedule(date, workShift, line, sequence, semiNo, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            DELETE PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeMixingSchedule Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得拌粉原料標籤
export async function getLabelMaterial(date, workShift, line, sequence, semiNo, user) {
    let obj = {
        res: [],
        bagWeight: 18, //配方下的單包紙袋重
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT MATERIAL, SUM(BATCH_WEIGHT) AS TOTAL_WEIGHT, RATIO
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY MATERIAL, RATIO `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const mixResult = await conn.execute(sql, params, options);
        obj.res = mixResult.rows;

        sql = `
            SELECT REC.BAG_WEIGHT
            FROM PBTC_IOT_RECIPE_DETAIL REC
                LEFT JOIN PRO_SCHEDULE PRO ON (
                    REC.LINE = PRO.LINE
                    AND REC.PRODUCT_NO = PRO.PRD_PC
                    AND REC.VER = PRO.SCH_SEQ
                    AND REC.COMPANY = :COMPANY
                    AND REC.FIRM = :FIRM )
            WHERE PRO.LINE = :LINE
            AND PRO.SEQ = :SEQUENCE
            AND PRO.COMPANY = :COMPANY
            AND PRO.FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const bagResult = await conn.execute(sql, params, options);
        if (bagResult.rows.length) {
            obj.bagWeight = bagResult.rows[0].BAG_WEIGHT;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getLabelMaterial Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得所有拌粉機清單
export async function getMixer(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT MIXER_NAME
            FROM PBTC_IOT_MIXER_INFO
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY MIXER_NAME `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMixer Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function createMixer(mixer, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_MIXER_INFO
            ( COMPANY, FIRM, MIXER_NAME, EDITOR)
            VALUES
            ( :COMPANY, :FIRM, :MIXER_NAME, :EDITOR) `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            MIXER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + mixer },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (error) {
        console.log(getNowDatetimeString(), 'createMixer Error', error);
        obj.res = error.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function deleteMixer(mixer, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            DELETE PBTC_IOT_MIXER_INFO
            WHERE MIXER_NAME = :MIXER_NAME
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            MIXER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + mixer },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });

    } catch (error) {
        console.log(getNowDatetimeString(), 'deleteMixer Error', error);
        obj.res = error.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得所有拌粉操作人員名單
export async function getMixOperator(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT PBTC_IOT_AUTH.PPS_CODE, PERSON_FULL.NAME
            FROM PBTC_IOT_AUTH, PERSON_FULL
            WHERE PBTC_IOT_AUTH.PPS_CODE = PERSON_FULL.PPS_CODE
            AND PERSON_FULL.IS_ACTIVE IN ('A', 'T')
            AND PBTC_IOT_AUTH.ROUTE = 'mixingPDA'
            AND PBTC_IOT_AUTH.COMPANY = :COMPANY
            AND PBTC_IOT_AUTH.FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMixOperator Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得所有列印標籤機台
export async function getAllPrinter(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT 
            DISTINCT PRINTER_NAME, PRINTER_IP
            FROM PBTC_IOT_PRINTER_INFO
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getAllPrinter', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//列印標籤紀錄
export async function printLabel(
    type, line, sequence, paperBagWeight, mixDate, workShift, batchStart, batchEnd,
    materials, printerIP, semiProductNo, semiProductWeight, semiNum, semiType, user) {

    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (let batch = batchStart; batch <= Math.ceil(batchEnd); batch++) {
            let batchRatio = 1; //正常狀況批數都是整數
            if (!Number.isInteger(batchEnd) && ((Math.ceil(batchEnd) === batch) || batchStart === batchEnd)) {
                batchRatio = Math.round((batchEnd % 1) * 100) / 100; //取小數再整理至小數點第二位
                batch = batchEnd;
            }

            //粉體原料部分
            let palletJSON = {};
            let currentLotNo = line + sequence + '-' + batch.toString();
            for (const material of materials) {
                if (1 !== batchRatio) {
                    const precision = (1 < material.BATCH_WEIGHT.split('.').length) ? material.BATCH_WEIGHT.split('.')[1].length : 0;
                    material.BATCH_WEIGHT = (Number(material.BATCH_WEIGHT) * batchRatio).toFixed(precision);
                }

                if ('normal' === type || 'powder' === type) {
                    const apiResult = await PrinterAPI.printLabelAPI(mixDate, workShift, currentLotNo, material.MATERIAL, material.BATCH_WEIGHT, material.UNIT, printerIP, 'MATERIAL', user);
                    if (!apiResult.error) {
                        if ('normal' === type) {
                            sql = `
                                UPDATE PBTC_IOT_MIX
                                SET LABEL_STATUS = 1,
                                    LABEL_DATE = :LABEL_DATE
                                WHERE MIX_DATE = :MIX_DATE
                                AND WORK_SHIFT = :WORK_SHIFT
                                AND LINE = :LINE
                                AND SEQUENCE = :SEQUENCE
                                AND MATERIAL = :MATERIAL
                                AND BATCH_SEQ = :BATCH_SEQ
                                AND COMPANY = :COMPANY
                                AND FIRM = :FIRM
                                AND SEMI_NO = :SEMI_NO `;
                            params = {
                                LABEL_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: currentLotNo[0].toString() },
                                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(currentLotNo.split('-')[0].slice(1)) },
                                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                                BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiProductNo[0] },
                                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                            };
                            await conn.execute(sql, params, { autoCommit: true });
                        }

                        //列印原料標籤紀錄
                        sql = `
                            INSERT INTO PBTC_IOT_MIX_LABEL_RECORD 
                            ( MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, MATERIAL, BATCH_SEQ, COUNT, RELABEL, COMPANY, FIRM )
                            VALUES
                            ( :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :MATERIAL, :BATCH_SEQ, :COUNT, :RELABEL, :COMPANY, :FIRM ) `;
                        params = {
                            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + workShift },
                            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: currentLotNo[0].toString() },
                            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(currentLotNo.split('-')[0].slice(1)) },
                            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material.MATERIAL },
                            BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                            COUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: 1 },
                            RELABEL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('powder' === type) ? 'Y' : 'N' },
                            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        };
                        await conn.execute(sql, params, { autoCommit: true });

                    } else {
                        throw new Error('列印標籤失敗', apiResult.res);
                    }

                } else if ('pallet' === type) {
                    //列印棧板原料標籤格式處理
                    palletJSON[material.MATERIAL] = material.BATCH_WEIGHT;
                }
            }

            if ('pallet' === type) {
                const apiResult = await PrinterAPI.printPalletAPI(mixDate, workShift, currentLotNo, palletJSON, printerIP, user);
                if (!apiResult.error) {
                    sql = `
                        UPDATE PBTC_IOT_MIX
                        SET LABEL_STATUS = 1,
                            LABEL_DATE = :LABEL_DATE
                        WHERE MIX_DATE = :MIX_DATE
                        AND WORK_SHIFT = :WORK_SHIFT
                        AND LINE = :LINE
                        AND SEQUENCE = :SEQUENCE
                        AND BATCH_SEQ = :BATCH_SEQ
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND SEMI_NO = :SEMI_NO `;
                    params = {
                        LABEL_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                        MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                        WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: currentLotNo[0].toString() },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(currentLotNo.split('-')[0].slice(1)) },
                        BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                        SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiProductNo[0] },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    await conn.execute(sql, params, { autoCommit: true });

                } else {
                    throw new Error('列印棧板標籤失敗', apiResult.res);
                }
            }

            //半成品簡碼列印部分
            if ('normal' === type || 'pallet' === type || 'semi' === type) {
                let bagWeight;
                let printCount = 0;
                for (let i = 0; i < semiNum; i++) {
                    //紙袋規則 => 總淨重/18(paperBagWeight)，例:50kg => 18, 18, 14，太空袋直接為批重
                    if ('paper' === semiType) {
                        bagWeight = (i === semiNum - 1) ? (semiProductWeight % paperBagWeight).toFixed(2) : paperBagWeight;
                    } else if ('fibc' === semiType) {
                        bagWeight = semiProductWeight;
                    }

                    const apiResult = await PrinterAPI.printLabelAPI(mixDate, workShift, currentLotNo, semiProductNo, bagWeight, 'kg', printerIP, 'MIX', user);
                    if (!apiResult.error) {
                        printCount++;

                    } else {
                        throw new Error('列印半成品標籤失敗');
                    }
                }

                sql = `
                    INSERT INTO PBTC_IOT_MIX_LABEL_RECORD 
                    ( MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, MATERIAL, BATCH_SEQ, COUNT, RELABEL, COMPANY, FIRM )
                    VALUES
                    ( :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :MATERIAL, :BATCH_SEQ, :COUNT, :RELABEL, :COMPANY, :FIRM ) `;
                params = {
                    MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                    WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: currentLotNo[0].toString() },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(currentLotNo.split('-')[0].slice(1)) },
                    MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiProductNo.toString() },
                    BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                    COUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(printCount) },
                    RELABEL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('semi' === type) ? 'Y' : 'N' },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                await conn.execute(sql, params, { autoCommit: true });
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printLabel Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//拌粉PDA部分
//取得拌粉原料領料
export async function getPickingMaterial(date, workShift, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let isAdmin = false;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查是否為管理員，能查看所有的備料狀態，
        sql = `
            SELECT ISADMIN
            FROM PBTC_IOT_AUTH
            WHERE ROUTE = 'mixingPDA'
            AND ISADMIN > 0
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PPS_CODE = :PPS_CODE `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        let adminResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (adminResult.rows.length) {
            isAdmin = true;
        }

        sql = `
            SELECT 
                PBTC_IOT_MIX.LINE, PBTC_IOT_MIX.SEQUENCE, PBTC_IOT_MIX.PRD_PC, 
                PBTC_IOT_MIX.MATERIAL, PBTC_IOT_MIX.SEMI_NO, PBTC_IOT_MIX.RATIO,
                MIN(PBTC_IOT_MIX.BATCH_SEQ) AS BATCH_START, 
                MAX(PBTC_IOT_MIX.BATCH_SEQ) AS BATCH_END, 
                SUM(PBTC_IOT_MIX.BATCH_WEIGHT) AS TOTAL_WEIGHT, 
                AVG(PBTC_IOT_MIX.PICK_NUM) AS PICK_NUM,
                AVG(PBTC_IOT_MIX.PICK_WEIGHT) AS PICK_WEIGHT,
                AVG(PBTC_IOT_MIX.PICK_STATUS) AS PICK_STATUS,
                AVG(PBTC_IOT_MIX.REMAINDER) AS REMAINDER,
                AVG(PBTC_IOT_REMAINDER.WEIGHT) AS REMAIN_WEIGHT
            FROM PBTC_IOT_MIX
                LEFT JOIN PBTC_IOT_REMAINDER ON ( 
                    PBTC_IOT_MIX.MATERIAL = PBTC_IOT_REMAINDER.MATERIAL 
                    AND PBTC_IOT_MIX.COMPANY = PBTC_IOT_REMAINDER.COMPANY
                    AND PBTC_IOT_MIX.FIRM = PBTC_IOT_REMAINDER.FIRM )
            WHERE PBTC_IOT_MIX.MIX_DATE = :MIX_DATE
            AND PBTC_IOT_MIX.WORK_SHIFT = :WORK_SHIFT
            AND ( PBTC_IOT_MIX.SEMI_NO = 'P' OR PBTC_IOT_MIX.SEMI_NO = 'G' )
            AND PBTC_IOT_MIX.COMPANY = :COMPANY
            AND PBTC_IOT_MIX.FIRM = :FIRM
            ${isAdmin ? '' : `AND PBTC_IOT_MIX.OPERATOR = '${'' + user.NAME}'`}
            GROUP BY PBTC_IOT_MIX.LINE, PBTC_IOT_MIX.SEQUENCE, PBTC_IOT_MIX.PRD_PC, PBTC_IOT_MIX.MATERIAL, PBTC_IOT_MIX.SEMI_NO, PBTC_IOT_MIX.RATIO
            ORDER BY PBTC_IOT_MIX.LINE, PBTC_IOT_MIX.SEQUENCE, PBTC_IOT_MIX.SEMI_NO `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;

        //檢查欲領用原料 包裝別(單包重)是否為固定數字，例如: 可能出現25kg一包、20kg一包的狀況
        for (let i = 0; i < obj.res.length; i++) {
            sql = `
                SELECT (QTY / PLQTY) AS PACK_WT
                FROM LOCINV_D
                WHERE PRD_PC = :MATERIAL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PLQTY != 0
                AND QTY != 0
                GROUP BY (QTY / PLQTY) `;
            params = {
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: obj.res[i].MATERIAL },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            obj.res[i].PACK_WT = adminResult.rows.map(x => x.PACK_WT);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPickingMaterial Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//拌粉領料/押出入料前先檢查原料棧板品檢結果
export async function materialBatchDetail(material, lotNo, batchNo, user) {
    let obj = {
        res: '',
        qa: '', //品檢結果
        firstIn: false, //是否為第一個進料
        remain: 0, //該棧板剩餘幾包
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //品檢結果
        const startTime = new Date();
        sql = `
            SELECT GET_QC_RESULTM(:COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO) AS RESULT
            FROM LOCINV_D
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('17P2' === user.DEPT) ? '17QA_IQC' : user.DEPT },
        };
        let locResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        const endTime = new Date();

        if (locResult.rows.length) {
            obj.qa = (locResult.rows[0].RESULT) ? locResult.rows[0].RESULT : '空值';
            console.log(`取得品檢值:${obj.qa}，耗時:${(endTime - startTime) / 1000}秒`);

            //棧板剩餘包數
            sql = `
                SELECT PLQTY, IN_DATE
                FROM LOCINV_D${('7' !== user.FIRM) ? '@ERPTEST' : ''}
                WHERE PRD_PC = :PRD_PC
                AND LOT_NO = :LOT_NO
                AND PAL_NO = :PAL_NO
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
                PAL_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + batchNo },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            let bagResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            obj.remain = bagResult.rows.length ? bagResult.rows[0].PLQTY : 0;

            //取得原料棧板的入庫順序
            sql = `
                SELECT IN_DATE
                FROM ( 
                    SELECT IN_DATE
                    FROM LOCINV_D${('7' !== user.FIRM) ? '@ERPTEST' : ''}
                    WHERE PRD_PC = :PRD_PC
                    AND PAL_NO IS NOT NULL
                    AND QTY > 0
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT
                    AND WAHS = :WAHS
                    ORDER BY IN_DATE
                )
                WHERE ROWNUM = 1 `;
            params = {
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('7' === user.FIRM) ? 'PT2' : '' }, //限制倉庫別
            };
            let inDateResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (inDateResult.rows.length && bagResult.rows.length) {
                if (bagResult.rows[0].IN_DATE && inDateResult.rows[0].IN_DATE) {
                    //可能是自建的標籤，有些原料IN_DATE會是NULL
                    obj.firstIn = (bagResult.rows[0].IN_DATE.valueOf() === inDateResult.rows[0].IN_DATE.valueOf());
                } else {
                    //待稽核完移除，自建的標籤沒有IN_DATE，帶回true
                    //obj.firstIn = true;
                }
            }

        } else {
            obj.res = '未找到該棧板編號';
            console.log(obj.res);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'materialBatchDetail Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//PDA掃描QRCode後確認領料
export async function pdaPicking(mixDate, pickShift, line, sequence, batchStart, batchEnd, semiNo, material, pickLotNo, pickBatchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查品檢結果，若是領餘料不需要檢查
        let qaResult = '';
        if (0 < bagPickNum) {
            //改為先檢查該原料應檢/免檢
            sql = `
                SELECT QC
                FROM PBTC_IOT_MATERIAL
                WHERE CODE = :MATERIAL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                //確認應檢再查品檢值
                if ('Y' === result.rows[0].QC) {
                    sql = `
                        SELECT GET_QC_RESULTM(:COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO) AS RESULT
                        FROM LOCINV_D
                        WHERE COMPANY = :COMPANY
                        AND FIRM = :FIRM `;
                    params = {
                        PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                        LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + pickLotNo },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('17P2' === user.DEPT) ? '17QA_IQC' : user.DEPT },
                    };
                    result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                    if (!result.rows.length) {
                        //throw new Error('查無此原料批號');
                    } else {
                        if ('Y' === result.rows[0].RESULT) {
                            qaResult = 'Y';
                        } else {
                            qaResult = 'N';
                            let sendMailsuccess = await Mailer.pickingAlarm(pickLotNo, pickBatchNo, line, sequence, material, qaResult, 'mixing', user);
                            if (sendMailsuccess) {
                                console.log('原料棧板尚未品檢，寄信完成');
                            } else {
                                console.error('原料棧板尚未品檢，寄信異常');
                            }
                        }
                    }

                } else {
                    console.log(`原料${material}免檢`);
                }
            }
        }

        //寫入餘料領料紀錄
        if (0 < remainderPickWeight) {
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( 
                    MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO, BATCH_SEQ_START, BATCH_SEQ_END,
                    LOT_NO, BATCH_NO, MATERIAL, WEIGHT, PICK_DATE, COMPANY, FIRM, PPS_CODE, NAME, QA_RESULT, STAGE )
                SELECT 
                    :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :SEMI_NO, :BATCH_SEQ_START, :BATCH_SEQ_END,
                    LOT_NO, BATCH_NO, :MATERIAL, :WEIGHT, :PICK_DATE, :COMPANY, :FIRM, :PPS_CODE, :NAME, 'RE', 'MIX'
                FROM PBTC_IOT_REMAINDER
                WHERE MATERIAL = :MATERIAL
                AND BATCH_NO IS NOT NULL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                BATCH_SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchStart.toString() },
                BATCH_SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchEnd.toString() },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderPickWeight) },
                PICK_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }

        //寫入棧板原料領料紀錄
        if (0 < bagPickWeight) {
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( 
                    MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO, BATCH_SEQ_START, BATCH_SEQ_END,
                    LOT_NO, BATCH_NO, MATERIAL, WEIGHT, PICK_NUM, PICK_DATE, COMPANY, FIRM, PPS_CODE, NAME, QA_RESULT, STAGE )
                VALUES ( 
                    :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :SEMI_NO, :BATCH_SEQ_START, :BATCH_SEQ_END,
                    :LOT_NO, :BATCH_NO, :MATERIAL, :WEIGHT, :BAG_PICK_NUM, :PICK_DATE, :COMPANY, :FIRM, :PPS_CODE, :NAME, :QA_RESULT, 'MIX' ) `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
                BATCH_SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchStart.toString() },
                BATCH_SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchEnd.toString() },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickLotNo ? pickLotNo.toString() : '' }, //半成品無原料批號
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickBatchNo.toString() }, //領料棧板編號
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickWeight) },
                BAG_PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
                PICK_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                QA_RESULT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: qaResult },
            };
            await conn.execute(sql, params, { autoCommit: false });

            //儲位扣帳
            sql = `
                UPDATE LOCINV_D${('7' !== user.FIRM) ? '@ERPTEST' : ''}
                SET QTY = QTY - ( PCK_KIND * :PICK_NUM ),
                    PLQTY = PLQTY - :PICK_NUM
                WHERE PAL_NO = :BATCH_NO
                AND PLQTY >= :PICK_NUM --收料作業早期的原料棧板'棧板數量'異常為0
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickBatchNo.toString() }, //領料棧板編號
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { autoCommit: false });
            if (!result.rowsAffected) {
                throw new Error('領用包數不可超過儲位棧板包數');
            }
        }

        //更新餘料量
        let remainderUpdateWeight = 0; //餘料量加減的數量
        let workPickWeight = 0; //工令領料量
        let pickStatus = 0; //是否領料完成
        if (totalNeedWeight <= (bagPickWeight + remainderPickWeight)) {
            remainderUpdateWeight = bagPickWeight - totalNeedWeight;
            workPickWeight = totalNeedWeight;
            pickStatus = 1;
        } else {
            remainderUpdateWeight = -remainderPickWeight;
            workPickWeight = bagPickWeight + remainderPickWeight;
        }

        //有領新包裝原料餘料桶更新為此次領料的批號，只領餘料不更新批號
        sql = `
            UPDATE PBTC_IOT_REMAINDER
            SET WEIGHT = WEIGHT + :REMAINDER_UPDATE_WEIGHT,
                EDITOR = :EDITOR
                ${(0 < bagPickWeight) ? `,BATCH_NO = '${pickBatchNo.toString()}'` : ''}
                ${(0 < bagPickWeight) ? `,LOT_NO = '${pickLotNo.toString()}'` : ''}
            WHERE MATERIAL = :MATERIAL `;
        params = {
            REMAINDER_UPDATE_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderUpdateWeight) },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            obj.res = '此原料尚未建立餘料桶';
            obj.error = true;
            return obj;
        }

        //更新工令領料量
        sql = `
            UPDATE PBTC_IOT_MIX
            SET PICK_NUM = PICK_NUM + :PICK_NUM,
                PICK_WEIGHT = PICK_WEIGHT + :PICK_WEIGHT,
                PICK_STATUS = :PICK_STATUS,
                REMAINDER = REMAINDER + :REMAINDER
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND MATERIAL = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
            PICK_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(workPickWeight) },
            PICK_STATUS: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(pickStatus) },
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            REMAINDER: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderPickWeight) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            obj.res = '未正常勾稽，請確認是否掃正確的QR Code標籤';
            obj.error = true;
            return obj;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'pdaPicking Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//拌粉領料全數完成後扣帳
export async function pickingDeduct(pickDate, pickShift, line, sequence, semiNo, deductArray, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //更新工令已領料完成狀態
        sql = `
            UPDATE PBTC_IOT_MIX
            SET PICK_STATUS = 1
            WHERE PICK_STATUS = 0
            AND MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(pickDate) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });

        if (0 === result.rowsAffected) {
            obj.res = '餘料已扣帳過';
            obj.error = true;
        } else {
            //餘料桶逐一扣帳，可能會沒COMMIT
            for (const material of deductArray) {
                let deductWeight = parseFloat(material.PICK_WEIGHT) - parseFloat(material.TOTAL_WEIGHT); //更新完成後數量 = 已領量 - 需求量 + 餘料桶量
                sql = `
                    UPDATE PBTC_IOT_REMAINDER
                    SET WEIGHT = :PICK_WEIGHT + WEIGHT
                    WHERE MATERIAL = :MATERIAL
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM `;
                params = {
                    PICK_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(deductWeight) },
                    MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                result = await conn.execute(sql, params, { autoCommit: false });
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'pickingDeduct Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得拌粉原料備料
export async function getStockMixing(date, workShift, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT LINE, SEQUENCE, PRD_PC, BATCH_SEQ, STOCK_STATUS, SEMI_NO, MIN(PICK_STATUS) AS PICK_STATUS
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE, PRD_PC, BATCH_SEQ, STOCK_STATUS, SEMI_NO
            ORDER BY LINE, SEQUENCE, BATCH_SEQ `;
        let params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getStockMixing Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得拌粉備料的原料重量
export async function getStockMixingMaterial(date, workShift, line, sequence, semiNo, batchSequence, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT LINE, SEQUENCE, PRD_PC, BATCH_SEQ, MATERIAL, BATCH_WEIGHT, RATIO
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            ${('*' === batchSequence) ? '' : `AND BATCH_SEQ = ${Number(batchSequence)}`}
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        let params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getStockMixingMaterial Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//拌粉備料確認，並Insert確認照片
export async function stockEnsure(image, stockDate, stockShift, line, sequence, semiNo, batch, user) {
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
            INSERT INTO PBTC_IOT_MIX_IMAGE ( IMAGE, MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO,${('*' === batch) ? '' : 'BATCH_SEQ,'} AUDITOR, COMPANY, FIRM )
            VALUES ( :IMAGE, :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :SEMI_NO,${('*' === batch) ? '' : Number(batch) + ','} :AUDITOR, :COMPANY, :FIRM ) `;
        params = {
            IMAGE: { dir: oracledb.BIND_IN, type: oracledb.BUFFER, val: image },
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(stockDate) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: stockShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            AUDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });

        sql = `
            UPDATE PBTC_IOT_MIX
            SET STOCK_STATUS = 1
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            ${('*' === batch) ? '' : `AND BATCH_SEQ = ${Number(batch)}`}
            AND SEMI_NO = :SEMI_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(stockDate) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: stockShift.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'stockEnsure Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得拌料確認的照片
export async function getEnsureImage(line, sequence, date, workShift, user) {
    let obj = {
        image: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT IMAGE
            FROM PBTC_IOT_MIX_IMAGE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + sequence },
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + workShift },
        };
        oracledb.fetchAsBuffer = [oracledb.BLOB];
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            const image = Buffer.from(result.rows[0].IMAGE).toString('base64');
            obj.image = image;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getEnsureImage Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得排程入料狀況
export async function getFeedStatus(date, workShift, user) {
    let obj = {
        orders: [], //所有工令
        detail: [], //工令下各原料的狀態
        error: false,
    };

    let conn;
    let sql;
    let params;
    let isAdmin = false;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查是否為管理員，能查看所有的備料狀態，
        sql = `
            SELECT ISADMIN
            FROM PBTC_IOT_AUTH
            WHERE ROUTE = 'mixingPDA'
            AND ISADMIN > 0
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PPS_CODE = :PPS_CODE `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        let adminResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (adminResult.rows.length) {
            isAdmin = true;
        }

        sql = `
            SELECT LINE, SEQUENCE, PRD_PC, BATCH_SEQ, MATERIAL, SEMI_NO, MIXER, BATCH_WEIGHT, FEED_STATUS, FEED_DATE, STOCK_STATUS, RATIO
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
            ${isAdmin ? '' : `AND OPERATOR = '${'' + user.NAME}'`}
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY LINE, SEQUENCE, BATCH_SEQ `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const detailResult = await conn.execute(sql, params, options);
        obj.detail = detailResult.rows;

        //切分入料未完成or完成的
        sql = `
            SELECT LINE, SEQUENCE, PRD_PC, BATCH_SEQ, SEMI_NO, AVG(FEED_STATUS) AS FEED_STATUS
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
            ${isAdmin ? '' : `AND OPERATOR = '${'' + user.NAME}'`}
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE, PRD_PC, BATCH_SEQ, SEMI_NO
            ORDER BY LINE, SEQUENCE, BATCH_SEQ `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const orderResult = await conn.execute(sql, params, options);
        obj.orders = orderResult.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFeedStatus Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//PDA掃描QRCode後確認入料
export async function pdaFeeding(type, mixDate, feedShift, line, sequence, material, batch, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        if ('pallet' === type) {
            sql = `
                UPDATE PBTC_IOT_MIX
                SET FEED_STATUS = 1,
                    FEED_DATE = :FEED_DATE,
                    FEED_USER = :FEED_USER
                WHERE MIX_DATE = :MIX_DATE
                AND WORK_SHIFT = :WORK_SHIFT
                AND LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND BATCH_SEQ = :BATCH_SEQ
                AND STOCK_STATUS = 1
                AND FEED_STATUS = 0
                AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                FEED_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                FEED_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feedShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        } else {
            sql = `
                UPDATE PBTC_IOT_MIX
                SET FEED_STATUS = 1,
                    FEED_DATE = :FEED_DATE,
                    FEED_USER = :FEED_USER
                WHERE MIX_DATE = :MIX_DATE
                AND WORK_SHIFT = :WORK_SHIFT
                AND LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND MATERIAL = :MATERIAL
                AND BATCH_SEQ = :BATCH_SEQ
                AND STOCK_STATUS = 1
                AND FEED_STATUS = 0
                AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                FEED_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
                FEED_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feedShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
                BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        }

        const commit = { autoCommit: false };
        const result = await conn.execute(sql, params, commit);
        if (0 === result.rowsAffected) {
            obj.res = '未正常勾稽，請確認是否掃正確的QR Code標籤，或已入料過';
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'pdaFeeding Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//漳州用，同時查詢領用量與入料狀況
export async function getMixWorkStatus(date, workShift, user) {
    let obj = {
        orders: [], //所有工令
        detail: [], //工令下各原料的狀態
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT 
                S0.LINE, S0.SEQUENCE, S0.PRD_PC, S0.BATCH_SEQ, S0.MATERIAL, S0.SEMI_NO, S0.MIXER, S0.FEED_STATUS, S0.FEED_DATE, S0.RATIO,
                S0.BATCH_WEIGHT AS NEED_WEIGHT, 
                S0.PICK_WEIGHT AS FEED_WEIGHT,
                S1.WEIGHT AS REMAIN_WEIGHT
            FROM PBTC_IOT_MIX S0 LEFT JOIN PBTC_IOT_REMAINDER S1
                ON S0.MATERIAL = S1.MATERIAL
                AND S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
            WHERE S0.MIX_DATE = :MIX_DATE
            AND S0.WORK_SHIFT = :WORK_SHIFT
            AND ( S0.SEMI_NO = 'P' OR S0.SEMI_NO = 'G' )
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            ORDER BY S0.LINE, S0.SEQUENCE, S0.BATCH_SEQ `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + workShift },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.detail = result.rows;

        //切分入料未完成or完成的
        sql = `
            SELECT LINE, SEQUENCE, PRD_PC, BATCH_SEQ, SEMI_NO, AVG(FEED_STATUS) AS FEED_STATUS
            FROM PBTC_IOT_MIX
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND ( SEMI_NO = 'P' OR SEMI_NO = 'G' )
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE, PRD_PC, BATCH_SEQ, SEMI_NO
            ORDER BY LINE, SEQUENCE, BATCH_SEQ `;
        params = {
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(date) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const orderResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.orders = orderResult.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMixWorkStatus', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//漳州廠與高雄廠稽核用，PDA掃描QRCode後直接做領料+入料
export async function pickAndFeed(mixDate, pickShift, line, sequence, batch, semiNo, material, lotNo, batchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查品檢結果，若是領餘料不需要檢查
        let qaResult = '';
        if (0 < bagPickNum) {
            //改為先檢查該原料應檢/免檢
            sql = `
                SELECT QC
                FROM PBTC_IOT_MATERIAL
                WHERE CODE = :MATERIAL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                //確認應檢再查品檢值
                if ('Y' === result.rows[0].QC) {
                    sql = `
                        SELECT GET_QC_RESULTM(:COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO) AS RESULT
                        FROM LOCINV_D
                        WHERE COMPANY = :COMPANY
                        AND FIRM = :FIRM `;
                    params = {
                        PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                        LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('17P2' === user.DEPT) ? '17QA_IQC' : user.DEPT },
                    };
                    result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                    if (!result.rows.length) {
                        //throw new Error('查無此原料批號');
                    } else {
                        if ('Y' === result.rows[0].RESULT) {
                            qaResult = 'Y';
                        } else {
                            qaResult = 'N';
                            let sendMailsuccess = await Mailer.pickingAlarm(lotNo, batchNo, line, sequence, material, qaResult, 'mixing', user);
                            if (sendMailsuccess) {
                                console.log('原料棧板尚未品檢，寄信完成');
                            } else {
                                console.error('原料棧板尚未品檢，寄信異常');
                            }
                        }
                    }

                } else {
                    console.log(`原料${material}免檢`);
                }
            }
        }

        //寫入餘料領料紀錄
        if (0 < remainderPickWeight) {
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( 
                    MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO, BATCH_SEQ_START, BATCH_SEQ_END,
                    LOT_NO, BATCH_NO, MATERIAL, WEIGHT, PICK_DATE, COMPANY, FIRM, PPS_CODE, NAME, QA_RESULT, STAGE )
                SELECT 
                    :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :SEMI_NO, :BATCH, :BATCH,
                    LOT_NO, BATCH_NO, :MATERIAL, :WEIGHT, SYSDATE, :COMPANY, :FIRM, :PPS_CODE, :NAME, 'RE', 'MIX'
                FROM PBTC_IOT_REMAINDER
                WHERE MATERIAL = :MATERIAL
                AND BATCH_NO IS NOT NULL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + pickShift },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + semiNo },
                BATCH: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + batch },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderPickWeight) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }

        //寫入棧板原料領料紀錄
        if (0 < bagPickWeight) {
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( 
                    MIX_DATE, WORK_SHIFT, LINE, SEQUENCE, SEMI_NO, BATCH_SEQ_START, BATCH_SEQ_END,
                    LOT_NO, BATCH_NO, MATERIAL, WEIGHT, PICK_NUM, PICK_DATE, COMPANY, FIRM, PPS_CODE, NAME, QA_RESULT, STAGE )
                VALUES ( 
                    :MIX_DATE, :WORK_SHIFT, :LINE, :SEQUENCE, :SEMI_NO, :BATCH, :BATCH,
                    :LOT_NO, :BATCH_NO, :MATERIAL, :WEIGHT, :BAG_PICK_NUM, SYSDATE, :COMPANY, :FIRM, :PPS_CODE, :NAME, :QA_RESULT, 'MIX' ) `;
            params = {
                MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + pickShift },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + semiNo },
                BATCH: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + batch },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: lotNo ? '' + lotNo : '' }, //半成品無原料批號
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + batchNo }, //領料棧板編號
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickWeight) },
                BAG_PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                QA_RESULT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: qaResult },
            };
            await conn.execute(sql, params, { autoCommit: false });

            //儲位扣帳
            sql = `
                UPDATE LOCINV_D
                SET QTY = QTY - ( PCK_KIND * :PICK_NUM ),
                    PLQTY = PLQTY - :PICK_NUM
                WHERE PAL_NO = :BATCH_NO
                AND PLQTY >= :PICK_NUM --收料作業早期的原料棧板'棧板數量'異常為0
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + batchNo }, //領料棧板編號
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { autoCommit: false });
            if (!result.rowsAffected) {
                throw new Error('領用包數不可超過儲位棧板包數');
            }
        }

        //更新餘料量
        let remainderUpdateWeight = 0; //餘料量加減的數量
        let workPickWeight = 0; //工令領料量
        let pickStatus = 0; //是否領料完成
        if (totalNeedWeight <= (bagPickWeight + remainderPickWeight)) {
            remainderUpdateWeight = bagPickWeight - totalNeedWeight;
            workPickWeight = totalNeedWeight;
            pickStatus = 1;
        } else {
            remainderUpdateWeight = -remainderPickWeight;
            workPickWeight = bagPickWeight + remainderPickWeight;
        }

        //有領新包裝原料餘料桶更新為此次領料的批號，只領餘料不更新批號
        sql = `
            UPDATE PBTC_IOT_REMAINDER
            SET WEIGHT = WEIGHT + :REMAINDER_UPDATE_WEIGHT,
                EDITOR = :EDITOR
                ${(0 < bagPickWeight) ? `,BATCH_NO = '${'' + batchNo}'` : ''}
                ${(0 < bagPickWeight) ? `,LOT_NO = '${'' + lotNo}'` : ''}
            WHERE MATERIAL = :MATERIAL `;
        params = {
            REMAINDER_UPDATE_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderUpdateWeight) },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            obj.res = '此原料尚未建立餘料桶';
            obj.error = true;
            return obj;
        }

        //更新工令領料，入料量
        sql = `
            UPDATE PBTC_IOT_MIX
            SET PICK_NUM = PICK_NUM + :PICK_NUM,
                PICK_WEIGHT = PICK_WEIGHT + :PICK_WEIGHT,
                PICK_STATUS = :PICK_STATUS,
                REMAINDER = REMAINDER + :REMAINDER,
                FEED_STATUS = :FEED_STATUS,
                FEED_DATE = SYSDATE
            WHERE MIX_DATE = :MIX_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND BATCH_SEQ = :BATCH_SEQ
            AND SEMI_NO = :SEMI_NO
            AND MATERIAL = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagPickNum) },
            PICK_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(workPickWeight) },
            PICK_STATUS: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(pickStatus) },
            MIX_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(mixDate) },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + pickShift },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batch) },
            REMAINDER: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainderPickWeight) },
            FEED_STATUS: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: (totalNeedWeight === workPickWeight) ? 1 : 0 },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + semiNo },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            obj.res = '未正常勾稽，請確認是否掃正確的QR Code標籤';
            obj.error = true;
            return obj;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'pickAndFeed Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (!obj.error) {
            await conn.commit();
        }
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

/* 生產排程 */
//取得指定部門的生產排程
export async function getSchedule(line, sequence, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        if (line && sequence) {
            sql = `
            SELECT LINE, SEQ, PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND SEQ = :SEQ
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: sequence.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        } else {
            sql = `
                SELECT * FROM (
                    SELECT LINE, PRD_PC, UKEY, SEQ, BATCH_NM, PRO_WT, STR_PRO_TIME, 
                    ROW_NUMBER() OVER ( ORDER BY PRO_SCHEDULE.STR_PRO_TIME DESC ) RNO
                    FROM PRO_SCHEDULE
                    WHERE PRO_SCHEDULE.COMPANY = :COMPANY
                    AND PRO_SCHEDULE.FIRM = :FIRM
                    ORDER BY STR_PRO_TIME DESC
                )
                WHERE RNO <= 30 `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        }

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSchedule Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
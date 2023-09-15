import config from '../config.js';
import { getNowDatetimeString, getInvShtNo, getInvInDateSeq, getInvtDate } from '../libs.js';
import oracledb from 'oracledb';
import * as VisionTagsAPI from '../VisionTagsAPI.js';
import moment from 'moment';
import axios from 'axios';

//依照配方比例計算小數點精度
function getMaterialPrecision(weight, ratio) {
    /*
    若配方比≧5 則原料領用量顯示至整數
    若配方比≧0.1 , <5  則原料領用量顯示至小數點後一位(修正)
    若配方比<0.1 則原料領用量顯示至小數點後四位
    */
    if (5 <= ratio) {
        return parseFloat(weight.toFixed(0));
    } else if (0.1 <= ratio && 5 > ratio) {
        return parseFloat(weight.toFixed(1));
    } else {
        return parseFloat(weight.toFixed(4));
    }
}

//取得棧板領料紀錄
export async function getPalletPicking(date, queryType, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        if ('date' === queryType) {
            sql = `
                SELECT 
                    RECORD.LINE, RECORD.SEQUENCE, RECORD.BATCH_NO, RECORD.MATERIAL, RECORD.WEIGHT, RECORD.PICK_NUM,
                    LOC.LOT_NO, RECORD.PICK_DATE, RECORD.PPS_CODE, RECORD.NAME
                FROM PBTC_IOT_PICKING_RECORD RECORD, LOCINV_D LOC
                WHERE RECORD.BATCH_NO = LOC.PAL_NO
                AND TO_CHAR( RECORD.PICK_DATE, 'YYYYMMDD' ) = :PICK_DATE
                AND RECORD.COMPANY = :COMPANY
                AND RECORD.FIRM = :FIRM 
                ORDER BY RECORD.PICK_DATE `;
        } else {
            sql = `
                SELECT
                    RECORD.LINE, RECORD.SEQUENCE, RECORD.BATCH_NO, RECORD.MATERIAL, RECORD.WEIGHT, RECORD.PICK_NUM,
                    LOC.LOT_NO, RECORD.PICK_DATE, RECORD.PPS_CODE, RECORD.NAME
                    FROM PBTC_IOT_PICKING_RECORD RECORD, LOCINV_D LOC
                WHERE RECORD.BATCH_NO = LOC.PAL_NO
                AND RECORD.PICK_DATE < SYSDATE
                AND RECORD.PICK_DATE > TO_DATE( :PICK_DATE, 'YYYYMMDD' )
                AND RECORD.COMPANY = :COMPANY
                AND RECORD.FIRM = :FIRM 
                ORDER BY RECORD.PICK_DATE `;
        }
        params = {
            PICK_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYYMMDD') },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPalletPicking Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得一個區間內的所有工令
async function getSchedulesByTime(startTime, endTime, type, user) {
    let result = [];
    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        if ('date' === type) {
            sql = `
                SELECT
                    S0.LINE, S0.SEQ, S0.SCH_SEQ, S0.PRD_PC, S0.LOT_NO, S0.PRO_WT, S0.WT_PER_HR, S0.ACT_STR_TIME, S0.ACT_END_TIME, REPLACE(S0.SILO, '-', '') AS SILO,
                    S1.VISION_START_TIME, S1.VISION_END_TIME
                FROM PRO_SCHEDULE S0 LEFT JOIN PBTC_IOT_SCHEDULE S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.LINE = S1.LINE
                    AND S0.SEQ = S1.SEQUENCE
                WHERE ( ( S0.ACT_END_TIME IS NOT NULL AND S1.VISION_END_TIME IS NULL ) OR S0.ACT_END_TIME IS NULL )
                AND S0.ACT_STR_TIME <= :END_TIME
                AND S0.ACT_STR_TIME > TO_DATE('20230530', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束? 
                AND S0.LOT_NO IS NOT NULL --近日有人在動批號，常常出現NULL
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                ORDER BY S0.LINE, S0.SEQ `;
            params = {
                END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(endTime) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };

        } else if ('quality' === type) {
            //20230215為搜尋20230215-至今(最多一週)完成的排程
            sql = `
                SELECT
                    LINE, SEQ, SCH_SEQ, PRD_PC, LOT_NO, PRO_WT, WT_PER_HR, ACT_STR_TIME, ACT_END_TIME, REPLACE(SILO, '-', '') AS SILO
                FROM PRO_SCHEDULE
                WHERE ( ACT_END_TIME > :START_TIME OR ACT_END_TIME IS NULL )
                AND ACT_STR_TIME <= :END_TIME
                AND ACT_STR_TIME > TO_DATE('20221001', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束? 
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                ORDER BY LINE, SEQ `;
            params = {
                START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(startTime) },
                END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(endTime) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        }
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    } catch (err) {
        console.error(getNowDatetimeString(), 'getSchedulesByTime', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return result;
}

//判斷實際開機時間
async function updateActualStartTime(line, sequence, startTime, user) {
    const continuousTime = 30; //兩筆工令間隔多久判斷為"連續生產"

    let visionStartTime = startTime;
    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //先找出上一筆工令判斷是否為"連續生產"(30分鐘內就生產下一筆工令)
        sql = `
            SELECT ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND SEQ = :SEQ - 1
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let lastScheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        if (lastScheduleResult.rows.length) {
            let continuously = true;
            if (moment(lastScheduleResult.rows[0].ACT_END_TIME).add(continuousTime, 'minutes').isBefore(moment(startTime))) {
                //不為連續生產之工令計算Vision實際開機時間(上限15分鐘)，連續生產之排程啟動時間直接為輸入時間
                continuously = false;
                let mongoResult = await VisionTagsAPI.getScheduleActualTime(line, 'START', startTime, continuousTime / 2, user);
                if (mongoResult.res.length) {
                    visionStartTime = mongoResult.res[0]['VISION_TIME'];
                }
            }

            sql = `
                INSERT INTO PBTC_IOT_SCHEDULE ( LINE, SEQUENCE, VISION_START_TIME, CONTINUOUSLY, COMPANY, FIRM )
                VALUES ( :LINE, :SEQUENCE, :VISION_START_TIME, :CONTINUOUSLY, :COMPANY, :FIRM ) `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                VISION_START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(visionStartTime) },
                CONTINUOUSLY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: continuously ? 'Y' : 'N' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: true });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateActualStartTime', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return visionStartTime;
}

//判斷實際關機時間，邏輯比判斷開機時間複雜很多
async function updateActualEndTime(line, sequence, endTime, user) {
    const continuousTime = 30; //兩筆工令間隔多久判斷為"連續生產"

    let visionEndTime = endTime;
    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //找出下一筆工令是否已輸入啟動時間，並判斷兩者相隔多久，可能會出現尚未排的狀況
        sql = `
            SELECT ACT_STR_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND ACT_STR_TIME IS NOT NULL
            AND SEQ = :SEQ + 1
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let nextScheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        let continuously = true;
        if (nextScheduleResult.rows.length) {
            //不為連續生產之工令計算Vision實際關機時間(上限15分鐘)，連續生產之排程啟動時間直接為輸入時間
            if (moment(nextScheduleResult.rows[0].ACT_STR_TIME).subtract(continuousTime, 'minutes').isAfter(moment(endTime))) {
                continuously = false;
            }

        } else {
            //超過30分鐘仍未設定下一筆的開始時間，上限就為15分鐘
            if (moment(endTime).add(continuousTime, 'minutes').isBefore(moment(new Date()))) {
                continuously = false;
            } else {
                return visionEndTime;
            }
        }

        //不為連續生產之工令計算Vision實際開機時間，連續生產之排程啟動時間直接為輸入時間
        if (!continuously) {
            let mongoResult = await VisionTagsAPI.getScheduleActualTime(line, 'END', endTime, continuousTime / 2, user);
            if (mongoResult.res.length) {
                visionEndTime = mongoResult.res[0]['VISION_TIME'];
            }
        }

        sql = `
            UPDATE PBTC_IOT_SCHEDULE
            SET VISION_END_TIME = :VISION_END_TIME
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            VISION_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(visionEndTime) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateActualEndTime', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return visionEndTime;
}

//線別序號查詢工令
async function getSchedulesByOrderLotNo(line, sequence, lotNo, queryType, user) {
    let result = [];
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT
                LINE, SEQ, SCH_SEQ, PRD_PC, LOT_NO, PRO_WT, WT_PER_HR, ACT_STR_TIME, ACT_END_TIME, REPLACE(SILO, '-', '') AS SILO
            FROM PRO_SCHEDULE
            WHERE ACT_STR_TIME < SYSDATE
            ${('lotNo' === queryType) ? `AND LOT_NO = '${lotNo}'` : ''}
            ${('order' === queryType) ? `AND LINE = '${line}' AND SEQ = ${Number(sequence)}` : ''}
            AND ACT_STR_TIME > TO_DATE('20221001', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束?
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        result = await conn.execute(sql, params, options);
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSchedulesByOrderLotNo', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return result;
}

//取得一個指定的配方
async function getRecipe(line, version, productNo, feederGroup, user) {
    let obj = [];
    let conn;
    let sql;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        if (feederGroup) {
            sql = `
                SELECT 
                    S0.FEEDER, S0.SEMI_NO, S1.TOLERANCE_RATIO,
                    SUM(S0.RATIO) AS RATIO,
                    LISTAGG(S0.MATERIAL, ', ') WITHIN GROUP (ORDER BY "MATERIAL") AS MATERIAL
                FROM PBTC_IOT_RECIPE S0 LEFT JOIN PBTC_IOT_FEEDER_INFO S1
                    ON S0.FEEDER = CONCAT(S1.LINE, S1.FEEDER)
                    AND S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                WHERE S0.PRODUCT_NO = :PRODUCT_NO
                AND S0.LINE = :LINE
                AND S0.VER = :VER
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                AND S0.CREATE_TIME = ( SELECT MAX( CREATE_TIME ) 
                                    FROM PBTC_IOT_RECIPE
                                    WHERE PRODUCT_NO = :PRODUCT_NO
                                    AND VER = :VER
                                    AND LINE = :LINE
                                    AND COMPANY = :COMPANY
                                    AND FIRM = :FIRM )
                GROUP BY S0.FEEDER, S0.SEMI_NO, S1.TOLERANCE_RATIO
                ORDER BY FEEDER `;
        } else {
            sql = `
                SELECT MATERIAL, RATIO, FEEDER, SEMI_NO
                FROM PBTC_IOT_RECIPE
                WHERE PRODUCT_NO = :PRODUCT_NO
                AND LINE = :LINE
                AND VER = :VER
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND CREATE_TIME = ( SELECT MAX( CREATE_TIME ) 
                                    FROM PBTC_IOT_RECIPE
                                    WHERE PRODUCT_NO = :PRODUCT_NO
                                    AND VER = :VER
                                    AND LINE = :LINE
                                    AND COMPANY = :COMPANY
                                    AND FIRM = :FIRM )
                ORDER BY FEEDER `;
        }
        const params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRecipe', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得配方的入料管制表，因入料機可能會在這邊變更
async function getFeedingForm(line, sequence, user) {
    let obj = [];
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT FEEDER_NO, MATERIAL, SEMI_NO
            FROM PBTC_IOT_EXTRUSION
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SEMI_NO IS NOT NULL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY FEEDER_NO `;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFeedingForm', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得工令包裝量
export async function getOrderPacking(line, sequence, startDate, endDate, user) {
    let result = [];
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT 
                MAX(S0.PRD_PC) AS PRD_PC,
                SUM((S1.DETAIL_SEQ_END - S1.DETAIL_SEQ_START - S1.SEQ_ERROR_COUNT + 1) * S0.PACKING_WEIGHT_SPEC) AS TOTAL_WEIGHT,
                LISTAGG(PACKING_NOTE, ', ') WITHIN GROUP (ORDER BY "PACKING_NOTE") AS PACK_NOTE,
                MAX(IS_EMPTYING) AS IS_EMPTYING,
                LISTAGG(PACKING_STATUS, ', ') WITHIN GROUP (ORDER BY "PACKING_STATUS") AS PACKING_STATUS,
                COUNT(*) AS ROW_NUM
            FROM AC.PBTC_IOT_PACKING_SCHEDULE S0 
                JOIN AC.PBTC_IOT_PACKING_DETAIL S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.PACKING_SEQ = S1.PACKING_SEQ
            WHERE 1 = 1
            AND S0.PRO_SCHEDULE_LINE = :LINE
            AND S0.PRO_SCHEDULE_SEQ = :SEQ
            ${(startDate && endDate) ?
        ` AND TRUNC(S0.PACKING_DATE) BETWEEN 
                TO_DATE(${moment(startDate).format('YYYYMMDD')}, 'YYYYMMDD') AND 
                TO_DATE(${moment(endDate).format('YYYYMMDD')}, 'YYYYMMDD') `
        :
        ''
}
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM `;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
    } catch (err) {
        console.error(getNowDatetimeString(), 'getOrderPacking', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return result;
}

//取得工令殘包產出量
export async function getOrderRemainBag(line, sequence, user) {
    let result = [];
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT
                SUM(S1.WEIGHT) AS REMAIN_BAG_WEIGHT
            FROM AC.PRO_SCHEDULE S0 
                LEFT JOIN AC.RM_STGFLD S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.LOT_NO = S1.LOT_NO
            WHERE 1 = 1
            AND S0.LINE = :LINE
            AND S0.SEQ = :SEQ
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM `;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
    } catch (err) {
        console.error(getNowDatetimeString(), 'getOrderRemainBag', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return result;
}

//取得工令前料、料頭使用量
export async function getOrderReworkWeight(line, sequence, startTime, endTime, user) {
    let obj = {
        scrap: 0,
        head: 0,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT SUM(WEIGHT) AS WEIGHT
            FROM PBTC_IOT_EXTR_SCRAP
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            ${(startTime && endTime) ? ' AND CREATE_TIME >= :START_TIME AND CREATE_TIME < :END_TIME ' : ''}
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        if (startTime && endTime) {
            params['START_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: startTime ? new Date(startTime) : new Date() };
            params['END_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: endTime ? new Date(endTime) : new Date() };
        }
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.scrap = result.rows[0].WEIGHT;
        }

        sql = `
            SELECT SUM(WEIGHT) AS WEIGHT
            FROM PBTC_IOT_EXTR_HEAD
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            ${(startTime && endTime) ? ' AND CREATE_TIME >= :START_TIME AND CREATE_TIME < :END_TIME ' : ''}
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        if (startTime && endTime) {
            params['START_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: startTime ? new Date(startTime) : new Date() };
            params['END_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: endTime ? new Date(endTime) : new Date() };
        }
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.head = result.rows[0].WEIGHT;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getOrderReworkWeight', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得領料或繳庫的單號
export async function getSheetNo(type, lotNo, user) {
    const obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT * FROM (
                SELECT SHTNO
                FROM PBTC_IOT_ERP_POSTING
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND LOT_NO = :LOT_NO
                AND SHEET_ID = '${'pick' === type ? 'PT1' : 'PT2'}'
                AND CREATOR = 'TPIOT'
                ORDER BY CREATE_TIME
            ) WHERE ROWNUM <= 1 `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.res = ('null' === result.rows[0].SHTNO) ? null : result.rows[0].SHTNO;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSheetNo Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }
    return obj;
}

//取得定期領繳數量
export async function getPickAndPay(timeStart, timeEnd, user) {
    const obj = {
        pick: [],
        pay: [],
        errorTags: [],
        errorRecipes: [],
        error: false,
    };

    try {
        //查詢這段期間生產中或已完成的排程
        const scheduleResult = await getSchedulesByTime(timeStart, timeEnd, 'date', user);

        for (let schedule of scheduleResult.rows) {
            console.log(`計算${schedule.LINE}-${schedule.SEQ}領繳量`);

            //剛啟動之排程，尚未計算VISION實際開機時間
            let scheduleTimeStart = schedule.ACT_STR_TIME;
            if (!schedule.VISION_START_TIME) {
                console.log(`計算${schedule.LINE}-${schedule.SEQ}於Vision實際開機時間`);
                scheduleTimeStart = await updateActualStartTime(schedule.LINE, schedule.SEQ, schedule.ACT_STR_TIME, user);
            } else if (schedule.VISION_START_TIME < schedule.ACT_STR_TIME) {
                scheduleTimeStart = schedule.VISION_START_TIME;
            }

            //未記錄實際結束時間排程
            let scheduleTimeEnd = timeEnd;
            if (!schedule.VISION_END_TIME && schedule.ACT_END_TIME && (schedule.ACT_END_TIME < timeEnd)) {
                console.log(`計算${schedule.LINE}-${schedule.SEQ}於Vision實際關機時間`);
                scheduleTimeEnd = await updateActualEndTime(schedule.LINE, schedule.SEQ, schedule.ACT_END_TIME, user);
            }

            //找配方檔查詢所使用的原料與對應的入料機
            const recipeResult = await getRecipe(schedule.LINE, schedule.SCH_SEQ, schedule.PRD_PC, false, user);

            //查詢入料管制表，因入料機可能在入料管至表切換
            const feedingForm = await getFeedingForm(schedule.LINE, schedule.SEQ, user);

            //至少要建立配方檔才能做自動領繳
            if (recipeResult.length) {
                //取得該工令已成功領繳至ERP的量，這次的領繳量 = 從工令開始生產累計至今領繳量 - 成功領繳至ERP的量
                const invtPickResult = await getInvtPick('lotNo', null, null, null, null, schedule.LOT_NO, user);
                const invtPayResult = await getInvtPay('lotNo', null, null, null, null, schedule.LOT_NO, user, false, false);
                const invtPayBefore = invtPayResult.res.length ? invtPayResult.res[0].FEED_STORAGE : 0;

                //取得從工令開始生產累計至今領繳量
                const mongoResult = await VisionTagsAPI.getAccumulateWeight(schedule.LINE, scheduleTimeStart, scheduleTimeEnd, false, user);
                if (mongoResult.error) {
                    throw new Error('MongoDB連線失敗');
                }

                if (mongoResult.res) {
                    let orderFeedTotal = 0; //從工令開始生產累計至今入料量
                    for (let i = 1; i <= mongoResult.feederFound; i++) {
                        //配方入料機設定
                        const recipeFeederSetting = recipeResult.filter(x => (x.FEEDER === (schedule.LINE + 'M' + i.toString())));

                        //入料管制表入料機設定
                        const formFeederSetting = feedingForm.filter(x => (x.FEEDER_NO === (schedule.LINE + 'M' + i.toString())));

                        //若配方與入料管制表都有設定該入料機，以入料管制表為準(若同一入料機設定多筆原料(半成品、中間桶)，則以配方為主)
                        const feederSetting = (feedingForm.length && (1 === recipeFeederSetting.length)) ? formFeederSetting : recipeFeederSetting;

                        //找出該入料機已成功領料至ERP的量
                        let feederFeedBefore = 0;
                        invtPickResult.res.forEach(x => {
                            if (x.FEEDER_NO === schedule.LINE + 'M' + i.toString()) {
                                feederFeedBefore += x.PICK_WEIGHT;
                            }
                        });

                        //檢查入料機是否是用半成品，計算半成品比例(入料管制表無法切換半成品入料機)
                        const semiTotalRatio = {
                            'P': recipeFeederSetting.filter(x => 'P' === x.SEMI_NO).reduce((accumulator, currentValue) => accumulator + currentValue.RATIO, 0),
                            'G': recipeFeederSetting.filter(x => 'G' === x.SEMI_NO).reduce((accumulator, currentValue) => accumulator + currentValue.RATIO, 0),
                        };

                        let feederFeedCurrent = 0; //入料機本次的入料量
                        for (const feederData of feederSetting) {
                            let pickWeightCurrent = 0; //原料本次的入料量
                            let pickWeightTotal = 0; //原料累計入料量

                            if ((mongoResult.res[`M${i}_maxWeight`] < mongoResult.res[`M${i}_minWeight`])) {
                                console.log(`入料機M${i}Tag出現斷訊、歸零，此次領料量為0`);
                                break;

                            } else if (feederFeedBefore > (mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`] + 0.05)) {
                                //若最終值-最初值小於過去量(會有小數點位數問題)不合理，可能是斷訊或歸零的問題，這批領繳0
                                console.log(`入料機M${i}Tag出現未使用或負斜率，此次領料量為0`);
                                break;

                            } else if ((mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`] - feederFeedBefore > 1800 * 3)) {
                                //根據配方檔，入料機加總每小時最大上限為1800，所以每10分鐘最多繳庫300，最多允許斷線3小時，超過3小時候再補也不領繳
                                console.log(`入料機M${i}Tag可能出現大幅跳動異常，此次領料量為0`);
                                break;
                            }

                            //處理半成品最終領繳量 = 理論值
                            if ('P' === feederData.SEMI_NO || 'G' === feederData.SEMI_NO) {
                                const materialFeedBefore = feederFeedBefore * feederData.RATIO / semiTotalRatio[feederData.SEMI_NO];

                                if (feederFeedBefore >= (schedule.PRO_WT * semiTotalRatio[feederData.SEMI_NO] / 100)) {
                                    //過去已達上限值，直接不領繳
                                    break;

                                } else if (((mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`]) >= (schedule.PRO_WT * semiTotalRatio[feederData.SEMI_NO] / 100)) || schedule.ACT_END_TIME) {
                                    //本次超過上限值或輸入結束，補到理論值
                                    pickWeightCurrent = (schedule.PRO_WT * feederData.RATIO / 100) - (feederFeedBefore * feederData.RATIO / semiTotalRatio[feederData.SEMI_NO]);
                                    pickWeightTotal = (schedule.PRO_WT * feederData.RATIO / 100);

                                } else {
                                    //未達上限值，持續領繳
                                    pickWeightCurrent = ((mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`]) - feederFeedBefore) * feederData.RATIO / semiTotalRatio[feederData.SEMI_NO];
                                    pickWeightTotal = materialFeedBefore + pickWeightCurrent;

                                }

                            } else {
                                pickWeightTotal = mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`];
                                pickWeightCurrent = pickWeightTotal - feederFeedBefore;

                            }

                            feederFeedCurrent += pickWeightCurrent;

                            //紀錄領料
                            obj.pick.push({
                                LINE: schedule.LINE,
                                SEQ: schedule.SEQ,
                                PRD_PC: schedule.PRD_PC,
                                LOT_NO: schedule.LOT_NO,
                                SEMI_NO: feederData.SEMI_NO, //領料API需要判斷是否為半成品
                                MATERIAL: feederData.MATERIAL,
                                FEEDER_NO: schedule.LINE + 'M' + i.toString(),
                                PICK_WEIGHT: getMaterialPrecision(pickWeightTotal, feederData.RATIO), //20230905改為累加值
                                OLDQTY: getMaterialPrecision(pickWeightTotal - pickWeightCurrent, feederData.RATIO), //領料不傳入，但要記錄
                            });
                        }

                        orderFeedTotal += feederFeedBefore + feederFeedCurrent;
                    }

                    obj.pay.push({
                        LINE: schedule.LINE,
                        SEQ: schedule.SEQ,
                        SCH_SEQ: schedule.SCH_SEQ,
                        PRD_PC: schedule.PRD_PC,
                        LOT_NO: schedule.LOT_NO,
                        SILO: schedule.SILO,
                        PRO_WT: schedule.PRO_WT,
                        WT_PER_HR: schedule.WT_PER_HR,
                        ACT_STR_TIME: schedule.ACT_STR_TIME,
                        ACT_END_TIME: schedule.ACT_END_TIME,
                        FEED_STORAGE: Math.round(orderFeedTotal), //20230905改為累加值
                        OLDQTY: Math.round(invtPayBefore),
                    });

                } else {
                    obj.errorTags.push(schedule.LINE + '-' + schedule.SEQ);
                }

            } else {
                obj.errorRecipes.push(schedule.LINE + '-' + schedule.SEQ);
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPickAndPay Error', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//定期自動執行押出領繳
export async function runPickAndPay(minutes, user) {
    const obj = {
        res: '',
        error: false,
    };

    const payApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_pbtc'; //游晟繳庫Procedure的API
    const pickApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟領料Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    try {
        //這次過帳時間區間要是多少
        const timeEnd = new Date();
        const timeStart = moment(timeEnd).subtract(minutes, 'minutes').set({ seconds: 0, milliseconds: 0 }).toDate();

        //如果時間為00:00~08:00則日期的帳轉到昨天的帳上
        const invtDate = getInvtDate(timeEnd);
        console.log(`執行自動領繳${moment(invtDate).format('YYYY/MM/DD')}，時間區間${moment(timeStart).format('MM/DD HH:mm')} ~ ${moment(timeEnd).format('MM/DD HH:mm')}`);

        let data = await getPickAndPay(timeStart, timeEnd, user);
        if (data.error) {
            throw new Error('領繳查詢異常');
        }

        //領料
        let currentSchedule = '';
        let shtNo = '';
        for (const pickData of data.pick) {
            const getOldSheetNo = await getSheetNo('pick', pickData.LOT_NO, user);
            //新的工令產生新的過帳單號
            if (currentSchedule !== pickData.LINE + pickData.SEQ) {
                if (getOldSheetNo.res) {
                    shtNo = getOldSheetNo.res;
                } else {
                    const getNewSheetNo = await getInvShtNo('PT1', invtDate);
                    if (getNewSheetNo.error) {
                        throw new Error('產生過帳單號失敗: ' + getNewSheetNo.error);
                    }
                    shtNo = getNewSheetNo.res;
                    currentSchedule = pickData.LINE + pickData.SEQ;
                    console.log(`未找到單號${getOldSheetNo.res}，將使用新單號${shtNo}`);
                }
            }

            let bodyData = [{
                'DEBUG': TEST_MODE, //(true測試/false正式)
                'SHEET_ID': 'PT1', //固定
                'SHTNO': shtNo, //getInvShtNo產生
                'INVT_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
                'PRD_PC': pickData.PRD_PC, //成品
                'MAT_PC': pickData.MATERIAL, //原料
                'PCK_KIND': 0, //固定
                'PCK_NO': '*', //固定
                'QTY': pickData.PICK_WEIGHT, //領料重量(累加值)
                'IN_QTY': 0, //固定
                'PM': ('OFFPBT01' === pickData.MATERIAL) ? 'P' : 'M', //(P成品/M原料)
                'LOT_NO': pickData.LOT_NO, //主排程的批號
                'CREATOR': '' + user.PPS_CODE, //會再調整為自動
            }];
            const apiResult = await axios.post(pickApiURL, bodyData, { proxy: false });
            //console.log(apiResult.data) //res = [ [ 'TEST8013701', '2', '存檔成功' ] ]
            console.log(`${pickData.LINE}-${pickData.SEQ}; 領料${pickData.MATERIAL}重${pickData.PICK_WEIGHT}; 之前累計值${pickData.OLDQTY}; ${apiResult.data[0][2]}`);

            bodyData[0].OLDQTY = pickData.OLDQTY; //API無法接受傳入OLDQTY，額外再加進來紀錄
            bodyData[0].WEIGHT = getOldSheetNo.res ? pickData.PICK_WEIGHT - pickData.OLDQTY : pickData.PICK_WEIGHT;
            await saveERPPostingRecord(user, timeStart, timeEnd, bodyData[0], apiResult.data[0][2], pickData.FEEDER_NO);

            //扣掉M1樹酯入料機的儲位量
            if ('1' === user.COMPANY && pickData.FEEDER_NO === pickData.LINE + 'M1' && 0 < pickData.PICK_WEIGHT) {
                await updateResinSiloInvt(pickData.LINE, pickData.SEQ, pickData.PICK_WEIGHT - pickData.OLDQTY, user);
            }
        }

        //繳庫
        for (const payData of data.pay) {
            //產生過帳單號
            const getOldSheetNo = await getSheetNo('pay', payData.LOT_NO, user);
            if (getOldSheetNo.res) {
                shtNo = getOldSheetNo.res;
            } else {
                const getNewSheetNo = await getInvShtNo('PT2', invtDate);
                if (getNewSheetNo.error) {
                    throw new Error('產生過帳單號失敗: ' + getNewSheetNo.error);
                }
                shtNo = getNewSheetNo.res;
                console.log(`未找到單號${getOldSheetNo.res}，將使用新單號${shtNo}`);
            }

            //相同批號(同工令)要使用同一個INDATESEQ
            let inDateSeq;
            let oldInDateSeq = await getLotNoInvtDate(user, payData.LOT_NO, payData.PRD_PC, payData.SILO);
            if (oldInDateSeq.inDateSeq.length) {
                inDateSeq = oldInDateSeq.inDateSeq;
            } else {
                //產生儲位入庫日期序號
                let inDateSeqResult = await getInvInDateSeq(moment(invtDate).format('YYYYMMDD'));
                if (inDateSeqResult.error) {
                    throw new Error('產生儲位入庫日期序號失敗: ' + inDateSeqResult.error);
                }
                inDateSeq = inDateSeqResult.res;
            }

            let bodyData = [{
                'DEBUG': TEST_MODE, //(true測試/false正式)
                'SHEET_ID': 'PT2',  //固定
                'SHTNO': shtNo, //getInvShtNo產生
                'INVT_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
                'LOC_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
                'PRD_PC': payData.PRD_PC, //成品
                'QTY': payData.FEED_STORAGE, //繳庫重量(累加值)
                'OLDQTY': getOldSheetNo.res ? payData.OLDQTY : payData.FEED_STORAGE,//傳上次前繳庫的累計值，第一次傳與QTY相同
                'SIGN': (0 > payData.FEED_STORAGE) ? '-' : '+', //繳庫正負號
                'LOT_NO': payData.LOT_NO, //主排程的批號
                'CCPCODE': 'E100', //E100一般繳庫、E171改番繳庫、E170重工繳庫
                'REMARK': '', //要過儲位帳直接傳空
                'CREATOR': '' + user.PPS_CODE, //會再調整為自動
                'INDATESEQ': inDateSeq, //getInvInDateSeq產生
                'LOC': payData.SILO //主排程的SILO1，原為7PLD2007
            }];
            const apiResult = await axios.post(payApiURL, bodyData, { proxy: false });
            //console.log(apiResult.data) //res = [ [ 'PT2231120001', null, '存檔成功' ] ]
            console.log(`${payData.LINE}-${payData.SEQ}; 繳庫重${payData.FEED_STORAGE}; 之前累計值${payData.OLDQTY}; ${apiResult.data[0][2]}`);

            bodyData[0].OLDQTY = getOldSheetNo.res ? bodyData[0].OLDQTY : 0; //ERP API規則第一次儲位要傳與繳庫量相同值，為方便記錄改為0
            bodyData[0].WEIGHT = getOldSheetNo.res ? payData.FEED_STORAGE - payData.OLDQTY : payData.FEED_STORAGE;
            await saveERPPostingRecord(user, timeStart, timeEnd, bodyData[0], apiResult.data[0][2]);
        }

    } catch (err) {
        console.error(getNowDatetimeString(), 'runPickAndPay Error', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//檢查批號在儲位中是否有INDATESEQ，LotNo可能會有多個INDATESEQ(打到不同的SILO)
export async function getLotNoInvtDate(user, lotNo, productNo, loc = null) {
    let obj = {
        inDateSeq: '',
        weight: 0,
        loc: '',
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT INDATESEQ, QTY, LOC
            FROM LOCINV_D@ERPTEST
            WHERE LOT_NO = :LOT_NO
            ${loc ? ` AND LOC = '${loc}' ` : ''} --是否指定儲位SILO
            AND WAHS = :WAHS
            ${productNo ? ` AND PRD_PC = '${productNo}' ` : ''} --避免抓到料頭前料的儲位
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY INDATESEQ DESC `;
        const params = {
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
            WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('1' === user.COMPANY) ? 'PT2' : '' }, //漳州廠待補
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.inDateSeq = result.rows[0].INDATESEQ;
            obj.qty = result.rows[0].QTY;
            obj.loc = result.rows[0].LOC;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getLotNoInvtDate', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//自動領料自動扣除M1樹酯SILO儲位
export async function updateResinSiloInvt(line, sequence, pickWeight, user) {
    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //先找出該工令使用的SILO
        sql = `
            SELECT SILO_NO, SILO_LOT_NO, SILO_PRD_PC
            FROM PBTC_IOT_EXTRUSION
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND SILO_NO IS NOT NULL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const siloResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (siloResult.rows.length) {
            const siloNo = siloResult.rows[0].SILO_NO;
            const siloLotNo = siloResult.rows[0].SILO_LOT_NO;
            const siloProductNo = siloResult.rows[0].SILO_PRD_PC;

            //更新該SILO的儲位帳，先進先出
            sql = `
                UPDATE LOCINV_D
                SET QTY = QTY - :PICK_WEIGHT
                WHERE LOC = :LOC
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND INDATESEQ = (
                    SELECT INDATESEQ
                    FROM (
                        SELECT INDATESEQ
                        FROM LOCINV_D 
                        WHERE LOC = :LOC
                        AND QTY > 0
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND DEPT = :DEPT
                        ORDER BY IN_DATE
                    )
                    WHERE ROWNUM = 1 ) `;
            params = {
                LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: siloNo },
                PICK_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(pickWeight) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            };
            await conn.execute(sql, params, { autoCommit: true });

            //儲存樹酯領料紀錄
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( LINE, SEQUENCE, SEMI_NO, MATERIAL, WEIGHT, LOT_NO, PICK_DATE, COMPANY, FIRM, PPS_CODE, NAME, STAGE, SILO )
                VALUES ( :LINE, :SEQUENCE, 'M', :MATERIAL, :WEIGHT, :LOT_NO, SYSDATE, :COMPANY, :FIRM, :PPS_CODE, :NAME, 'EXTRUSION', :SILO )`;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + siloProductNo },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(pickWeight) },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + siloLotNo },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                SILO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + siloNo },
            };
            await conn.execute(sql, params, { autoCommit: true });

        } else {
            console.log('M1樹酯入料機SILO設定異常');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateResinSiloInvt', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return;
}

//取得押出繳庫紀錄
export async function getInvtPay(queryType, date, line, start, end, lotNo, user, groupBySilo = true, calculateRework = true) {
    const obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //李部長希望查詢時，即使當日沒生產的排程，但凡有繳庫(未生產但有料頭、前料)的都抓出來，但這樣就看不出"生產中切換的儲位"
        let sql = `
            SELECT 
                S1.PRD_PC, S1.LOT_NO ${groupBySilo ? ', S1.SILO AS LOC' : ''}, MAX(S0.QTY) - MIN(S0.OLDQTY) AS FEED_STORAGE, MAX(SHTNO) AS LASTEST_SHT_NO,
                S1.LINE, S1.SEQ, S1.SCH_SEQ, S1.PRO_WT, S1.WT_PER_HR, S1.ACT_STR_TIME, S1.ACT_END_TIME,
                S2.VISION_START_TIME, S2.VISION_END_TIME
            FROM PBTC_IOT_ERP_POSTING S0 
                JOIN PRO_SCHEDULE S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.LOT_NO = S1.LOT_NO
                    AND S0.PRD_PC = S1.PRD_PC
                LEFT JOIN PBTC_IOT_SCHEDULE S2
                    ON S1.COMPANY = S2.COMPANY
                    AND S1.FIRM = S2.FIRM
                    AND S1.LINE = S2.LINE
                    AND S1.SEQ = S2.SEQUENCE
            WHERE S0.SHEET_ID IN ( 'PT2', 'PT5' )
            AND S0.FEEDER_NO IS NULL --過濾該批號被當重工入料的料頭、前料、回爐品
            ${calculateRework ? '' : `
                AND ( S0.REWORK != 'FEED' OR S0.REWORK IS NULL )
            `}
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('date' === queryType) {
            //日期查詢排除顯示重工品
            sql += ` 
                AND S0.INVT_DATE = :INVT_DATE AND ( S0.REWORK != 'FEED' OR S0.REWORK IS NULL ) `;
            params['INVT_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYYMMDD') };

        } else if ('week' === queryType) {
            sql += ' AND S0.INVT_DATE >= :INVT_DATE ';
            params['INVT_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYYMMDD') };

        } else if ('order' === queryType) {
            sql += ' AND S1.LINE = :LINE AND S1.SEQ >= :SEQ_START AND S1.SEQ <= :SEQ_END ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
            params['SEQ_START'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(start) };
            params['SEQ_END'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(end) };

        } else if ('lotNo' === queryType) {
            sql += ' AND S0.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };

        } else if ('time' === queryType) {
            //專門為生產日報寫的，seqStart為班別起始時間，seqEnd為班別結束時間
            sql += ' AND S0.LOT_NO = :LOT_NO AND S0.END_TIME > :START_TIME AND S0.END_TIME <= :END_TIME ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };
            params['START_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(start) };
            params['END_TIME'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(end) };

        } else {
            throw new Error('錯誤的查詢');

        }

        sql += `
            GROUP BY 
                S1.PRD_PC, S1.LOT_NO ${groupBySilo ? ', S1.SILO' : ''}, S1.LINE, S1.SEQ, S1.SCH_SEQ, 
                S1.PRO_WT, S1.WT_PER_HR, S1.ACT_STR_TIME, S1.ACT_END_TIME,
                S2.VISION_START_TIME, S2.VISION_END_TIME
            ORDER BY S1.LINE, S1.SEQ `;
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        for (const schedule of result.rows) {
            let scheduleTimeStart = schedule.ACT_STR_TIME;
            let scheduleTimeEnd = schedule.ACT_END_TIME || new Date();
            if ('date' === queryType) {
                scheduleTimeStart = moment(date).set({ hour: 8, minute: 0, second: 0, }).toDate();
                scheduleTimeEnd = moment(date).add(1, 'day').set({ hour: 8, minute: 0, second: 0, }).toDate();
            } else if ('week' === queryType) {
                scheduleTimeStart = moment(date).set({ hour: 8, minute: 0, second: 0, }).toDate();
                scheduleTimeEnd = new Date();
            } else {
                scheduleTimeStart = null;
                scheduleTimeEnd = null;
            }

            //該工令料頭、前料使用量
            let reworkResult = await getOrderReworkWeight(schedule.LINE, schedule.SEQ, scheduleTimeStart, scheduleTimeEnd, user);
            schedule['SCRAP_WEIGHT'] = reworkResult.scrap;
            schedule['HEAD_WEIGHT'] = reworkResult.head;

            //該工令包裝量
            const packResult = await getOrderPacking(schedule.LINE, schedule.SEQ, scheduleTimeStart, scheduleTimeEnd, user);
            if (packResult.rows.length) {
                schedule['PACK_WEIGHT'] = packResult.rows[0].TOTAL_WEIGHT;
                schedule['IS_EMPTYING'] = packResult.rows[0].IS_EMPTYING;
                const remainBagResult = await getOrderRemainBag(schedule.LINE, schedule.SEQ, user);
                if (remainBagResult.rows.length) {
                    schedule['REMAIN_BAG_WEIGHT'] = remainBagResult.rows.length ? remainBagResult.rows[0].REMAIN_BAG_WEIGHT : 0;
                }

                if (packResult.rows[0].PACK_NOTE) {
                    if (packResult.rows[0].PACK_NOTE.includes('出空')) {
                        schedule['PACK_NOTE'] = '出空';
                    }
                }
            }

            obj.res.push(schedule);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getInvtPay Error', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//取得押出領料紀錄
export async function getInvtPick(queryType, date, line, seqStart, seqEnd, lotNo, user, distinctSystem = false) {
    const obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT 
                S0.PRD_PC, S0.MAT_PC, S0.LOT_NO, MAX(S0.QTY) - MIN(S0.OLDQTY) AS PICK_WEIGHT, S0.FEEDER_NO, MAX(SHTNO) AS LASTEST_SHT_NO,
                S1.LINE, S1.SEQ, S1.SCH_SEQ, S1.PRO_WT, S1.ACT_STR_TIME, S1.ACT_END_TIME, S2.RATIO
            FROM PBTC_IOT_ERP_POSTING S0 
                JOIN PRO_SCHEDULE S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.LOT_NO = S1.LOT_NO
                LEFT JOIN PBTC_IOT_RECIPE S2
                    ON S0.COMPANY = S2.COMPANY
                    AND S0.FIRM = S2.FIRM
                    AND S0.MAT_PC = S2.MATERIAL
                    AND S1.LINE = S2.LINE
                    AND S1.PRD_PC = S2.PRODUCT_NO
                    AND S1.SCH_SEQ = S2.VER
            WHERE S0.SHEET_ID = 'PT1'
            ${distinctSystem ? `
                AND S0.CREATOR = 'TPIOT'
            ` : ''}
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('date' === queryType) {
            sql += ' AND S0.INVT_DATE = :INVT_DATE ';
            params['INVT_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYYMMDD') };

        } else if ('week' === queryType) {
            sql += ' AND S0.INVT_DATE >= :INVT_DATE ';
            params['INVT_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYYMMDD') };

        } else if ('order' === queryType) {
            sql += ' AND S1.LINE = :LINE AND S1.SEQ >= :SEQ_START AND S1.SEQ <= :SEQ_END ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
            params['SEQ_START'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(seqStart) };
            params['SEQ_END'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(seqEnd) };

        } else if ('lotNo' === queryType) {
            sql += ' AND S0.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };

        } else {
            throw new Error('錯誤的查詢');

        }

        sql += `
            GROUP BY S0.PRD_PC, S0.MAT_PC, S0.LOT_NO, S0.FEEDER_NO, S1.LINE, S1.SEQ, S1.SCH_SEQ, S1.PRO_WT, S1.ACT_STR_TIME, S1.ACT_END_TIME, S2.RATIO
            ORDER BY S1.LINE, S1.SEQ, S0.FEEDER_NO `;
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getInvtPick Error', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//紀錄押出領繳過帳
export async function saveERPPostingRecord(user, timeStart, timeEnd, row, result, feederNo = null, rework = null) {
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_ERP_POSTING ( 
                DEBUG, SHEET_ID, SHTNO, INVT_DATE, PRD_PC, MAT_PC, QTY, OLDQTY, WEIGHT, FEEDER_NO, LOT_NO, LOC, REWORK, 
                CREATOR, INDATESEQ, RESULT, START_TIME, END_TIME, COMPANY, FIRM )
            VALUES ( 
                :DEBUG, :SHEET_ID, :SHTNO, :INVT_DATE, :PRD_PC, :MAT_PC, :QTY, :OLDQTY, :WEIGHT, :FEEDER_NO, :LOT_NO, :LOC, :REWORK, 
                :CREATOR, :INDATESEQ, :RESULT, :START_TIME, :END_TIME, :COMPANY, :FIRM ) `;
        const params = {
            DEBUG: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.DEBUG },
            SHEET_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.SHEET_ID },
            SHTNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.SHTNO },
            INVT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.INVT_DATE },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.PRD_PC },
            MAT_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.MAT_PC || 'NULL' },
            FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feederNo || '' },
            QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.QTY) },
            OLDQTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.OLDQTY || 0) },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WEIGHT || 0) },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.LOT_NO },
            LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOC || '' },
            REWORK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: rework || '' },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.CREATOR },
            INDATESEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.INDATESEQ || '' },
            RESULT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (typeof 'string' === result) ? result.substring(0, 60) : '' },
            START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(timeStart) },
            END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(timeEnd) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'saveERPPostingRecord', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return;
}

//押出入料品質表(押出機各原料入料重量比與標準配方的差異)
export async function getExtrusionQuality(date, queryType, user) {
    let obj = {
        res: [],
        error: false,
    };

    try {
        const startTime = moment(date).subtract(1, 'day').set({ hour: 8, second: 0, millisecond: 0 });
        const endTime = moment(date).set({ hour: 8, second: 0, millisecond: 0 });

        let scheduleResult = await getSchedulesByTime(startTime, endTime, 'quality', user);

        let returnArray = [];
        for (let schedule of scheduleResult.rows) {
            let scheduleArray = [];
            let todayStartTime;
            let todayEndTime;
            if ('date' === queryType) {
                todayStartTime = (schedule.ACT_STR_TIME < startTime) ? startTime : schedule.ACT_STR_TIME;
                todayEndTime = (!schedule.ACT_END_TIME || (schedule.ACT_END_TIME > endTime)) ? endTime : schedule.ACT_END_TIME;
            } else {
                //日期至今(最多一週內)完成的排程
                todayStartTime = schedule.ACT_STR_TIME;
                todayEndTime = schedule.ACT_END_TIME ? schedule.ACT_END_TIME : new Date();
            }

            const recipeResult = await getRecipe(schedule.LINE, schedule.SCH_SEQ, schedule.PRD_PC, true, user);
            if (recipeResult.length) {
                let mongoResult = await VisionTagsAPI.getAccumulateWeight(schedule.LINE, todayStartTime, todayEndTime, false, user);
                if (mongoResult.error) {
                    throw new Error('MongoDB連線失敗');
                }

                //console.log(mongoResult);
                if (mongoResult.res) {
                    let allFeederTotal = 0; //加總今天這個工令所有入料機的入料量
                    for (let i = 1; i <= mongoResult.feederFound; i++) {
                        let materialName = '';
                        let feederSettings = recipeResult.filter(x => (x.FEEDER === (schedule.LINE + 'M' + i.toString()))); //找出相符的入料機
                        let pickWeight = 0;
                        for (const feederData of feederSettings) {
                            //若該入料機為半成品，則原料名稱切換成半成品簡碼
                            if ('M' === feederData.SEMI_NO) {
                                materialName = feederData.MATERIAL;
                            } else {
                                materialName = feederData.SEMI_NO + schedule.PRD_PC;
                            }

                            pickWeight = mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`];
                            allFeederTotal += pickWeight;

                            scheduleArray.push({
                                LINE: schedule.LINE,
                                SEQ: schedule.SEQ,
                                SCH_SEQ: schedule.SCH_SEQ,
                                PRD_PC: schedule.PRD_PC,
                                FEEDER_NO: schedule.LINE + 'M' + i.toString(),
                                MATERIAL: materialName,
                                RATIO: feederData.RATIO,
                                TOLERANCE_RATIO: feederData.TOLERANCE_RATIO,
                                PICK_WEIGHT: pickWeight,
                                PICK_RATIO: 0, //最後再塞 PICK_WEIGHT / allFeederTotal進來計算百分比
                                PICK_DIFF: 0, //(PICK_RATIO - RATIO) * 100 / RATIO
                            });
                        }
                    }

                    scheduleArray.forEach((element, index, array) => {
                        //防止 PICK_WEIGHT / allFeederTotal 的 PICK_WEIGHT 出現0
                        if (0 !== element.PICK_WEIGHT) {
                            const pickRatio = (element.PICK_WEIGHT / allFeederTotal) * 100;
                            const pickDiff = (pickRatio - element.RATIO) * 100 / element.RATIO;
                            array[index].RATIO = parseFloat(element.RATIO).toFixed(2); //李部長要求統一小數點第2位
                            array[index].PICK_RATIO = parseFloat(pickRatio).toFixed(2);
                            array[index].PICK_DIFF = parseFloat(pickDiff).toFixed(2);
                        } else {
                            array[index].RATIO = parseFloat(element.RATIO).toFixed(2);
                            array[index].PICK_RATIO = 0;
                            array[index].PICK_DIFF = parseFloat(-100).toFixed(2);
                        }
                    });
                    returnArray = returnArray.concat(scheduleArray);
                }
            }
        }
        obj.res = returnArray;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getExtrusionQuality', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//取得指定工令的押出入料品質
export async function getOrderQuality(line, seqStart, seqEnd, lotNo, queryType, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (let sequence = seqStart; sequence <= seqEnd; sequence++) {
            let orderArray = [];

            result = await getSchedulesByOrderLotNo(line, sequence, lotNo, queryType, user);
            if (!result.rows.length) {
                throw new Error('排程不存在或尚未開始');
            }
            const scheduleResult = result.rows[0];
            const startTime = moment(scheduleResult.ACT_STR_TIME);
            const endTime = (scheduleResult.ACT_END_TIME) ? moment(scheduleResult.ACT_END_TIME) : moment(new Date());

            const recipeResult = await getRecipe(scheduleResult.LINE, scheduleResult.SCH_SEQ, scheduleResult.PRD_PC, true, user);
            if (!recipeResult.length) {
                throw new Error('無建立該工令配方檔');
            }

            let mongoResult = await VisionTagsAPI.getAccumulateWeight(scheduleResult.LINE, startTime, endTime, false, user);
            if (mongoResult.error) {
                throw new Error('MongoDB連線失敗');
            }

            if (mongoResult.res) {
                let allFeederTotal = 0; //加總今天這個工令所有入料機的入料量
                for (let i = 1; i <= mongoResult.feederFound; i++) {
                    let materialName = '';
                    let feederSettings = recipeResult.filter(x => (x.FEEDER === (scheduleResult.LINE + 'M' + i.toString()))); //找出相符的入料機
                    let pickWeight = 0;
                    for (const feederData of feederSettings) {
                        //若該入料機為半成品，則原料名稱切換成半成品簡碼
                        if ('M' === feederData.SEMI_NO) {
                            materialName = feederData.MATERIAL;
                        } else {
                            materialName = feederData.SEMI_NO + scheduleResult.PRD_PC;
                        }

                        pickWeight = mongoResult.res[`M${i}_maxWeight`] - mongoResult.res[`M${i}_minWeight`];
                        allFeederTotal += pickWeight;

                        orderArray.push({
                            LINE: scheduleResult.LINE,
                            SEQ: scheduleResult.SEQ,
                            SCH_SEQ: scheduleResult.SCH_SEQ,
                            PRD_PC: scheduleResult.PRD_PC,
                            FEEDER_NO: line + 'M' + i.toString(),
                            MATERIAL: materialName,
                            RATIO: feederData.RATIO,
                            TOLERANCE_RATIO: feederData.TOLERANCE_RATIO,
                            PICK_WEIGHT: pickWeight,
                            PICK_RATIO: 0, //最後再塞 PICK_WEIGHT / allFeederTotal進來計算百分比
                            PICK_DIFF: 0, //(PICK_RATIO - RATIO) * 100 / RATIO
                        });
                    }
                }

                orderArray.forEach((element, index, array) => {
                    //防止PICK_WEIGHT / allFeederTotal，防止PICK_WEIGHT出現0
                    if (0 !== element.PICK_WEIGHT) {
                        const pickRatio = (element.PICK_WEIGHT / allFeederTotal) * 100;
                        const pickDiff = (pickRatio - element.RATIO) * 100 / element.RATIO;
                        array[index].RATIO = parseFloat(element.RATIO).toFixed(2); //李部長要求統一小數點第2位
                        array[index].PICK_RATIO = parseFloat(pickRatio).toFixed(2);
                        array[index].PICK_DIFF = parseFloat(pickDiff).toFixed(2);
                    } else {
                        array[index].RATIO = parseFloat(element.RATIO).toFixed(2);
                        array[index].PICK_RATIO = 0;
                        array[index].PICK_DIFF = parseFloat(-100).toFixed(2);
                    }
                });

                obj.res = obj.res.concat(orderArray);

            } else {
                obj.res = '工令入料機異常';
                obj.error = true;
            }
        }

    } catch (err) {
        console.error(getNowDatetimeString(), 'getOrderQuality Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得重工/改番重量
export async function getLotNoReworkFeed(lotNo, user) {
    let obj = {
        res: 0,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT SUM(S0.QTY) AS QTY
            FROM PBTC_IOT_ERP_POSTING S0 JOIN PRO_SCHEDULE S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.LOT_NO = S1.LOT_NO
            WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.LOT_NO = :LOT_NO
            AND S0.REWORK IN ( 'REMAINBAG', 'FEED' ) `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
        };
        const reworkResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = reworkResult.rows.length ? reworkResult.rows[0].QTY : 0;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getLotNoInvtDetail Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得指定批號的領繳總量
export async function getLotNoInvtDetail(lotNo, user) {
    let obj = {
        pick: [],
        pickAdjust: [],
        pay: [],
        payLatest: 0,
        productNo: '',
        productWeight: 0,
        reworkWeight: 0,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //先檢查是否已經完成或存在
        sql = `
            SELECT S0.ACT_STR_TIME, S0.ACT_END_TIME, S0.PRO_WT, S0.PRD_PC, S1.VISION_START_TIME, S1.VISION_END_TIME
            FROM PRO_SCHEDULE S0 LEFT JOIN PBTC_IOT_SCHEDULE S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.LINE = S1.LINE
                AND S0.SEQ = S1.SEQUENCE
            WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.LOT_NO = :LOT_NO `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
        };
        const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!scheduleResult.rows.length) {
            throw new Error('未找到排程');
        }
        const scheduleData = scheduleResult.rows[0];
        if (!scheduleData.VISION_END_TIME) {
            throw new Error('Vision尚未計算排程完成時間');
        }
        const invtPickResult = await getInvtPick('lotNo', null, null, null, null, lotNo, user, true);
        const invtPayResult = await getInvtPay('lotNo', null, null, null, null, lotNo, user, false);

        //領料取得最新一筆不為系統建立的
        sql = `
            SELECT S0.MAT_PC, S0.QTY AS PICK_WEIGHT
            FROM PBTC_IOT_ERP_POSTING S0
            WHERE S0.SHEET_ID = 'PT1'
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.LOT_NO = :LOT_NO
            AND S0.CREATOR != 'TPIOT'
            AND S0.START_TIME = (
                SELECT MAX(START_TIME) 
                FROM PBTC_IOT_ERP_POSTING
                WHERE SHEET_ID = 'PT1'
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND LOT_NO = :LOT_NO
                AND CREATOR != 'TPIOT' ) `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
        };
        const adjustPickResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        //繳庫取得最新一筆
        sql = `
            SELECT * FROM (
                SELECT S0.QTY AS FEED_STORAGE
                FROM PBTC_IOT_ERP_POSTING S0
                WHERE S0.SHEET_ID = 'PT2'
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                AND S0.LOT_NO = :LOT_NO
                ORDER BY S0.CREATE_TIME DESC
            ) WHERE ROWNUM <= 1 `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
        };
        const adjustPayResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        obj.pick = !invtPickResult.error ? invtPickResult.res : [];
        obj.pickAdjust = adjustPickResult.rows.length ? adjustPickResult.rows : [];
        obj.pay = !invtPayResult.error ? invtPayResult.res : [];
        obj.payLatest = adjustPayResult.rows.length ? adjustPayResult.rows[0].FEED_STORAGE : 0;
        obj.productWeight = scheduleData.PRO_WT;
        obj.productNo = scheduleData.PRD_PC;

        const reworkFeed = await getLotNoReworkFeed(lotNo, user);
        obj.reworkWeight = reworkFeed.res;

    } catch (err) {
        console.error(getNowDatetimeString(), 'getLotNoInvtDetail Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//調整批號的領繳量
export async function adjustPickAndPay(lotNo, productNo, rows, user) {
    let obj = {
        res: '',
        error: false,
    };

    const payApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_pbtc'; //游晟繳庫Procedure的API
    const pickApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟領料Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    try {
        const invtDate = getInvtDate(new Date());

        //領料，共用一個單號
        const pickSheetNoResult = await getSheetNo('pick', lotNo, user);
        if (!pickSheetNoResult.res) {
            throw new Error('取得領料單號異常');
        }
        const pickSheetNo = pickSheetNoResult.res;

        const paySheetNoResult = await getSheetNo('pay', lotNo, user);
        if (!paySheetNoResult.res) {
            throw new Error('取得繳庫單號異常');
        }
        const paySheetNo = paySheetNoResult.res;

        for (const materialRow of rows) {
            let bodyData = [{
                'DEBUG': TEST_MODE, //(true測試/false正式)
                'SHEET_ID': 'PT1', //固定
                'SHTNO': pickSheetNo, //getInvShtNo產生
                'INVT_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
                'PRD_PC': productNo, //成品
                'MAT_PC': materialRow.MATERIAL, //原料
                'PCK_KIND': 0, //固定
                'PCK_NO': '*', //固定
                'QTY': materialRow.PICK_WEIGHT_ADJUST_AFTER, //領料重量
                'IN_QTY': 0, //固定
                'PM': ('OFFPBT01' === materialRow.MATERIAL) ? 'P' : 'M', //(P成品/M原料)
                'LOT_NO': lotNo, //主排程的批號
                'CREATOR': '' + user.PPS_CODE,
            }];
            const apiResult = await axios.post(pickApiURL, bodyData, { proxy: false });
            console.log(`批號${lotNo}; 調整領料${materialRow.MATERIAL}${apiResult.data[0][2]}; 重量${materialRow.PICK_WEIGHT_ADJUST_AFTER}`);

            bodyData[0].OLDQTY = materialRow.PICK_WEIGHT_ADJUST_BEFORE;
            bodyData[0].WEIGHT = materialRow.PICK_WEIGHT_ADJUST_BEFORE - materialRow.PICK_WEIGHT_ADJUST_BEFORE;
            await saveERPPostingRecord(user, invtDate, invtDate, bodyData[0], apiResult.data[0][2], materialRow.FEEDER_NO);
        }

        //繳庫
        //產生儲位入庫日期序號，此時包裝應已出空無該批號之儲位
        const inDateSeqResult = await getInvInDateSeq(moment(invtDate).format('YYYYMMDD'));
        if (inDateSeqResult.error) {
            throw new Error('產生儲位入庫日期序號失敗: ' + inDateSeqResult.error);
        }
        const inDateSeq = inDateSeqResult.res;

        let bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': 'PT2',  //固定
            'SHTNO': paySheetNo, //getInvShtNo產生
            'INVT_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(invtDate).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': productNo, //成品
            'QTY': rows[0].FEED_WEIGHT_ADJUST, //繳庫重量
            'OLDQTY': rows[0].FEED_WEIGHT,//傳上次前繳庫的累計值
            'SIGN': (0 > rows[0].FEED_WEIGHT) ? '-' : '+', //繳庫正負號
            'LOT_NO': lotNo, //主排程的批號
            'CCPCODE': 'E100', //E100一般繳庫、E171改番繳庫、E170重工繳庫
            'REMARK': 'N_LOC', //不過儲位帳傳N_LOC
            'CREATOR': '' + user.PPS_CODE,
            'INDATESEQ': inDateSeq, //getInvInDateSeq產生
            'LOC': 'PBTCADJUST' //調整用儲位，追溯用
        }];
        const apiResult = await axios.post(payApiURL, bodyData, { proxy: false });
        console.log(`批號${lotNo}; 調整繳庫${apiResult.data[0][2]}; 重量${rows[0].FEED_WEIGHT_ADJUST}`);

        bodyData[0].WEIGHT = rows[0].FEED_WEIGHT_ADJUST - rows[0].FEED_WEIGHT;
        await saveERPPostingRecord(user, invtDate, invtDate, bodyData[0], apiResult.data[0][2]);

    } catch (err) {
        console.error(getNowDatetimeString(), 'adjustPickAndPay Error', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}
import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import oracledb from 'oracledb';
import * as storageDB from './oracleStorage.js';
import * as VisionTagsAPI from '../VisionTagsAPI.js';
import * as Mailer from '../mailer.js';
import moment from 'moment';

/* 押出作業製造表 */
//取得工令現有押出作業製造表
export async function getForm(tableType, line, sequence, user) {
    const obj = {
        res: [],
        productNo: '',
        lotNo: '',
        silo: '',
        startTime: '', //排程啟動時間
        endTime: '', //排程結束時間
        remainBagLoNo: '', //殘包格位
        remainBagWeight: '', //殘包重量
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查排程是否已經結束
        sql = `
            SELECT PRD_PC, LOT_NO, SILO, ACT_STR_TIME, ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND SEQ = :SEQ
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!result.rows.length) {
            throw new Error('查無此排程');
        }
        obj.productNo = result.rows[0].PRD_PC;
        obj.lotNo = result.rows[0].LOT_NO;
        obj.silo = result.rows[0].SILO;
        obj.startTime = result.rows[0].ACT_STR_TIME;
        obj.endTime = result.rows[0].ACT_END_TIME;

        //查詢已建立的押出製造標準/製程檢驗表
        sql = `
            SELECT 
                CREATE_TIME,
                LISTAGG(STD_SEQUENCE, ',') WITHIN GROUP (ORDER BY STD_SEQUENCE) AS STD_SEQUENCE,
                LISTAGG(STD_VALUE, ',') WITHIN GROUP (ORDER BY STD_SEQUENCE) AS STD_VALUE
            FROM PBTC_IOT_EXTRUSION_FORM
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND RECORD_TYPE = :RECORD_TYPE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY CREATE_TIME `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            RECORD_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: tableType.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getForm', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//產品簡碼與日期區間查詢
export async function getOrder(line, productNo, startDate, endDate, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查排程是否已經結束
        const sql = `
            SELECT LINE, SEQ, PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE TO_CHAR( ACT_STR_TIME, 'YYYYMMDD' ) >= :START_DATE
            AND TO_CHAR( ACT_STR_TIME, 'YYYYMMDD' ) <= :END_DATE
            ${'*' === line ? '' : `AND LINE = '${line.toString()}'`}
            ${'*' === productNo ? '' : `AND PRD_PC = '${productNo.toString()}'`}
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY ACT_STR_TIME DESC `;
        const params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getOrder', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存工令的一筆製造紀錄
export async function saveForm(tableType, line, sequence, stdArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date(); //建立時間統一
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查排程是否已經結束
        sql = `
            SELECT PRD_PC, ACT_STR_TIME, ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND SEQ = :SEQ
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!result.rows.length) {
            throw new Error('查無此排程');
        }
        if (result.rows[0].ACT_END_TIME) {
            throw new Error('排程已經結束，無法再紀錄');
        }

        //檢查距離上一次檢查是否符合區間
        /*
        sql = `
            SELECT 
                MAX(CREATE_TIME) AS LAST_UPDATE_TIME
            FROM PBTC_IOT_EXTRUSION_FORM
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE, COMPANY, FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        }
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            if (moment(result.rows[0].LAST_UPDATE_TIME).add(240 - 60, 'minutes') > moment(date)) {
                throw new Error('請於上次儲存的4小時後再儲存下一筆');
            } else if (moment(result.rows[0].LAST_UPDATE_TIME).add(240 + 60, 'minutes') < moment(date)) {
                //將寄信通知太久才儲存下一筆，但仍可儲存。
            }
        }
        */

        if ('EXTR' === tableType) {
            //取得有多少個模頭溫度Tags，B線押出機溫度從2段開始、模頭溫度只有1個
            let visionTags = await VisionTagsAPI.mapOpcTags(line, user);
            let dieTagsNum = visionTags.dieTags.length;

            //讀取押出機溫度0~12、押出機模頭13-14、轉速15、負載16的TAGS，因押出機溫度可能只有1~8個
            let mongoResult = await VisionTagsAPI.getExtruderData(line, user);
            if (mongoResult.data.error) {
                console.log(mongoResult.data);
                throw new Error(`讀取押出機OPC數據錯誤，${mongoResult.data.error}`);
            }

            //把tags值map到要儲存的Array
            let tagsValue = Object.values(mongoResult.data.tags);
            let dieTagsIndex = 13;
            let tempTagsIndex = ('B' === line) ? 1 : 0;
            //console.log(tagsValue); //[轉速、負載or電流、模頭0~2個、溫度0~13個不等]
            for (let i = 0; i < tagsValue.length; i++) {
                if (i === 0) {
                    stdArray[15] = tagsValue[0]; //轉速

                } else if (i === 1) {
                    stdArray[16] = tagsValue[1]; //負載or電流

                } else if (i >= 2) {
                    if (dieTagsNum) {
                        stdArray[dieTagsIndex++] = tagsValue[i]; //模頭溫度
                        dieTagsNum--;
                    } else {
                        stdArray[tempTagsIndex++] = tagsValue[i]; //押出機溫度
                    }
                }
            }
        }

        for (let i = 0; i < stdArray.length; i++) {
            sql = `
                INSERT INTO PBTC_IOT_EXTRUSION_FORM ( LINE, SEQUENCE, STD_SEQUENCE, STD_VALUE, CREATE_TIME, CREATOR, RECORD_TYPE, COMPANY, FIRM )
                VALUES ( :LINE, :SEQUENCE, :STD_SEQUENCE, :STD_VALUE, :CREATE_TIME, :CREATOR, :RECORD_TYPE, :COMPANY, :FIRM ) `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                STD_SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: i + 1 },
                STD_VALUE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: stdArray[i].toString() },
                CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                RECORD_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: tableType },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { autoCommit: false });
            if (!result.rowsAffected) {
                throw new Error('儲存異常');
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'saveForm', err);
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

//修改押出製造/製程檢驗表
export async function updateForm(tableType, line, sequence, stdArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const createTime = stdArray.shift();
        for (let i = 0; i < stdArray.length; i++) {
            const sql = `
                UPDATE PBTC_IOT_EXTRUSION_FORM
                SET STD_VALUE = :STD_VALUE
                WHERE LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND RECORD_TYPE = :RECORD_TYPE
                AND CREATE_TIME = :CREATE_TIME
                AND STD_SEQUENCE = :STD_SEQUENCE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            const params = {
                STD_VALUE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: stdArray[i].toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                RECORD_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: tableType },
                CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(createTime) },
                STD_SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: i + 1 },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateForm', err);
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

//取得包裝SILO
export async function getPackingSilo(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT SILO_NAME 
            FROM AC.PBTC_IOT_PACKING_SILO 
            WHERE SILO_NAME NOT LIKE '%換包' 
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY SILO_ORDER ASC `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingSilo', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得電錶抄錶紀錄
export async function getMeterRecord(date, workShift, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT *
            FROM PBTC_IOT_METER_RECORD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND RECORD_DATE = :RECORD_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            ORDER BY
                CASE
                    WHEN METER_NO = '純水' THEN 1
                    WHEN METER_NO = 'AIR' THEN 2
                    WHEN METER_NO = '廢水' THEN 3
                    ELSE 0
                END,
                METER_NO ASC `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + date },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + workShift },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMeterRecord', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存/更新電錶抄錶紀錄
export async function saveMeterRecord(date, workShift, recordArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (const row of recordArray) {
            sql = `
                BEGIN
                    INSERT INTO PBTC_IOT_METER_RECORD (
                        RECORD_DATE, WORK_SHIFT, METER_NO, MULTIPLE, METER_VALUE, METER_LINE, 
                        COMPANY, FIRM, DEPT, CREATOR, CREATOR_NAME )
                    VALUES (
                        :RECORD_DATE, :WORK_SHIFT, :METER_NO, :MULTIPLE, :METER_VALUE, :METER_LINE, 
                        :COMPANY, :FIRM, :DEPT, :CREATOR, :CREATOR_NAME );
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE PBTC_IOT_METER_RECORD
                        SET MULTIPLE = :MULTIPLE,
                            METER_VALUE = :METER_VALUE,
                            METER_LINE = :METER_LINE,
                            CREATOR = :CREATOR,
                            CREATOR_NAME = :CREATOR_NAME,
                            CREATE_TIME = SYSDATE
                        WHERE RECORD_DATE = :RECORD_DATE
                        AND WORK_SHIFT = :WORK_SHIFT
                        AND METER_NO = :METER_NO
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND DEPT = :DEPT;
                END; `;
            params = {
                RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                METER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + row.METER_NO },
                MULTIPLE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.MULTIPLE ? row.MULTIPLE : 1) },
                METER_VALUE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.METER_VALUE ? row.METER_VALUE : 0) },
                METER_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.METER_LINE || '' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                CREATOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            };
            await conn.execute(sql, params, { autoCommit: true });
        }

    } catch (err) {
        console.error(getNowDatetimeString(), 'saveMeterRecord', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得交接紀錄表
export async function getHandoverRecord(date, workShift, user) {
    let obj = {
        res: [],
        error: false,
    };

    const workShiftTime = getWorkShiftTime(date, workShift);
    if (workShiftTime.startTime > new Date()) {
        obj.res = '該班別區間尚未開始';
        obj.error = true;
        return obj;
    }

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT 
                LINE, IC_NAME, EXTRUDER_STATUS, STOP_REASON, SEQ, PRD_PC, PRODUCTION_TIME, EXTRUDED_TIME, 
                SILO, PRODUCTIVITY, UNPACK, STOP_TIME, TOTAL_STOP_TIME, NOTE
            FROM PBTC_IOT_HANDOVER_FORM 
            WHERE RECORD_DATE = :RECORD_DATE --YYYYMMDD
            AND WORK_SHIFT = :WORK_SHIFT
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY LINE `;
        params = {
            RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let formResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        if (formResult.rows.length) {
            obj.res = formResult.rows;

        } else {
            /* 尚未紀錄，將自動抓資料並回傳 */
            //找出所有線別，再去對應線別找押出機"當下狀態"、生產規格、經時、押時、SILO、已生產量、未包裝量
            sql = 'SELECT LINE FROM PBTC_IOT_FEEDER_INFO WHERE COMPANY = :COMPANY AND FIRM = :FIRM GROUP BY LINE';
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            const lineResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

            let appendArray = [];
            for (const line of lineResult.rows) {
                //找出所有最後一筆生產的排程
                sql = `
                    SELECT * FROM (
                        SELECT LINE, SEQ, PRD_PC, LOT_NO, SPEND_TIME, SILO, ACT_STR_TIME, ACT_END_TIME
                        FROM PRO_SCHEDULE
                        WHERE LINE = :LINE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND ACT_STR_TIME IS NOT NULL
                        AND CRT_DATE > TO_DATE('20221001', 'YYYYMMDD')
                        ORDER BY SEQ DESC )
                    WHERE ROWNUM = 1 `;
                params = {
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.LINE },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

                //取得押出機當時段的"最後狀態"，改抓ERP排程是否啟動中
                // let extruderStatus = await VisionTagsAPI.getExtrusionStatus(line.LINE, new Date(), user);

                if (scheduleResult.rows.length) {
                    const scheduleEndTime = scheduleResult.rows[0].ACT_END_TIME || new Date();

                    //生產經時
                    const orderProductionTime = await VisionTagsAPI.getProductionTime(line.LINE, scheduleResult.rows[0].ACT_STR_TIME, scheduleEndTime, user);

                    //實際產量
                    const payResult = await storageDB.getInvtPay('lotNo', null, null, null, null, scheduleResult.rows[0].LOT_NO, user);
                    const feedWeight = payResult.res.length ? payResult.res[0].FEED_STORAGE : 0;

                    //取得工令包裝量
                    sql = `
                        SELECT SUM((S1.DETAIL_SEQ_END - S1.DETAIL_SEQ_START - S1.SEQ_ERROR_COUNT + 1) * S0.PACKING_WEIGHT_SPEC) AS TOTAL_WEIGHT
                        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0 JOIN AC.PBTC_IOT_PACKING_DETAIL S1
                            ON S0.COMPANY = S1.COMPANY
                            AND S0.FIRM = S1.FIRM
                            AND S0.PACKING_SEQ = S1.PACKING_SEQ
                        WHERE 1 = 1
                        AND S0.PRO_SCHEDULE_LINE = :LINE
                        AND S0.PRO_SCHEDULE_SEQ = :SEQ
                        AND S0.COMPANY = :COMPANY
                        AND S0.FIRM = :FIRM `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.LINE },
                        SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: scheduleResult.rows[0].SEQ },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    const packResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

                    appendArray.push({
                        LINE: line.LINE,
                        SEQ: scheduleResult.rows[0].SEQ,
                        PRD_PC: scheduleResult.rows[0].PRD_PC,
                        EXTRUDED_TIME: scheduleResult.rows[0].SPEND_TIME,
                        SILO: scheduleResult.rows[0].SILO,
                        EXTRUDER_STATUS: scheduleResult.rows[0].ACT_END_TIME ? '停俥' : '開俥',
                        STOP_REASON: '',
                        PRODUCTION_TIME: orderProductionTime.productionTime,
                        PRODUCTIVITY: feedWeight,
                        UNPACK: packResult.rows.length ? feedWeight - packResult.rows[0].TOTAL_WEIGHT : feedWeight,
                    });

                } else {
                    appendArray.push({
                        LINE: line.LINE,
                        SEQ: '',
                        PRD_PC: '',
                        EXTRUDED_TIME: '',
                        SILO: '',
                        EXTRUDER_STATUS: '停俥',
                        STOP_REASON: '',
                        PRODUCTION_TIME: 0,
                        PRODUCTIVITY: 0,
                        UNPACK: 0,
                    });
                }
            }

            appendArray.push({ LINE: '領班' }, { LINE: '檢驗' }, { LINE: '入料3' }, { LINE: '入料4' }, { LINE: '拌粉' }, { LINE: '包裝' });
            obj.res = appendArray;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getHandoverRecord', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存交接紀錄表
export async function saveHandoverRecord(date, workShift, recordArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    const workShiftTime = getWorkShiftTime(date, workShift);
    if (!moment(new Date()).isBetween(moment(workShiftTime.endTime).subtract(1, 'hour'), moment(workShiftTime.endTime).add(1, 'hour'))) {
        obj.res = '已超出交接時間，將無法儲存';
        obj.error = true;
        return obj;
    }

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (const row of recordArray) {
            sql = `
                BEGIN
                    INSERT INTO PBTC_IOT_HANDOVER_FORM (
                        RECORD_DATE, WORK_SHIFT, LINE,
                        IC_NAME, EXTRUDER_STATUS, STOP_REASON, SEQ, PRD_PC, PRODUCTION_TIME, EXTRUDED_TIME,
                        SILO, PRODUCTIVITY, UNPACK, STOP_TIME, TOTAL_STOP_TIME, NOTE,
                        COMPANY, FIRM, DEPT, CREATOR, CREATE_TIME )
                    VALUES (
                        :RECORD_DATE, :WORK_SHIFT, :LINE, 
                        :IC_NAME, :EXTRUDER_STATUS, :STOP_REASON, :SEQ, :PRD_PC, :PRODUCTION_TIME, :EXTRUDED_TIME,
                        :SILO, :PRODUCTIVITY, :UNPACK, :STOP_TIME, :TOTAL_STOP_TIME, :NOTE,
                        :COMPANY, :FIRM, :DEPT, :CREATOR, SYSDATE );
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE PBTC_IOT_HANDOVER_FORM
                        SET IC_NAME = :IC_NAME,
                            EXTRUDER_STATUS = :EXTRUDER_STATUS,
                            STOP_REASON = :STOP_REASON,
                            STOP_TIME = :STOP_TIME,
                            TOTAL_STOP_TIME = :TOTAL_STOP_TIME,
                            NOTE = :NOTE,
                            CREATOR = :CREATOR
                        WHERE RECORD_DATE = :RECORD_DATE
                        AND WORK_SHIFT = :WORK_SHIFT
                        AND LINE = :LINE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND DEPT = :DEPT;
                END; `;
            params = {
                RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LINE ? row.LINE : '' },
                IC_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.IC_NAME ? row.IC_NAME : '' },
                EXTRUDER_STATUS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.EXTRUDER_STATUS ? row.EXTRUDER_STATUS : '' },
                STOP_REASON: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.STOP_REASON ? row.STOP_REASON : '' },
                SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.SEQ ? row.SEQ : 0) },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC ? row.PRD_PC : '' },
                PRODUCTION_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.PRODUCTION_TIME ? row.PRODUCTION_TIME : 0) },
                EXTRUDED_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.EXTRUDED_TIME ? row.EXTRUDED_TIME : 0) },
                SILO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SILO ? row.SILO : '' },
                PRODUCTIVITY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.PRODUCTIVITY ? row.PRODUCTIVITY : 0) },
                UNPACK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.UNPACK ? row.UNPACK : 0) },
                STOP_TIME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.STOP_TIME ? row.STOP_TIME : '' },
                TOTAL_STOP_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.TOTAL_STOP_TIME ? row.TOTAL_STOP_TIME : 0) },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOTE || '' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            };
            await conn.execute(sql, params, { autoCommit: true });
        }

        //處理完的再寄信
        sql = `
            SELECT *
            FROM PBTC_IOT_HANDOVER_FORM
            WHERE RECORD_DATE = :RECORD_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY LINE`;
        params = {
            RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let formResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (formResult.rows.length) {
            Mailer.saveHandoverForm(date, workShift, formResult.rows, user);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'saveHandoverRecord', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得各班別的日期時間
export function getWorkShiftTime(date, workShift, earlyGet = false) {
    let obj = {
        startTime: '',
        endTime: '',
    };

    switch (workShift) {
        case '早':
            //8:00-16:00
            obj.startTime = moment(date).set({ hour: 8 }).toDate();
            obj.endTime = moment(date).set({ hour: 16 }).toDate();
            break;
        case '中':
            //16:00-00:00
            obj.startTime = moment(date).set({ hour: 16 }).toDate();
            obj.endTime = moment(date).set({ hour: 16 }).add(8, 'hours').toDate();
            break;
        case '夜':
        case '晚':
            //隔天的00:00-08:00
            obj.startTime = moment(date).add(1, 'day').set({ hour: 0 }).toDate();
            obj.endTime = moment(date).add(1, 'day').set({ hour: 8 }).toDate();
            break;
        default:
            //整日查詢
            obj.startTime = moment(date).set({ hour: 8 }).toDate();
            obj.endTime = moment(date).add(1, 'day').set({ hour: 8 }).toDate();
            break;
    }

    if (earlyGet) {
        obj.startTime = moment(obj.startTime).subtract(30, 'minutes').toDate();
        obj.endTime = moment(obj.endTime).subtract(30, 'minutes').toDate();
    }

    return obj;
}

//取得上一班
export function getLastWorkShift(date, workShift) {
    let obj = {
        date: '',
        workShift: '',
    };

    switch (workShift) {
        case '早':
            obj.date = moment(date, 'YYYYMMDD').subtract(1, 'day').format('YYYYMMDD');
            obj.workShift = '晚';
            break;
        case '中':
            obj.date = date;
            obj.workShift = '早';
            break;
        case '夜':
        case '晚':
            obj.date = date;
            obj.workShift = '中';
            break;
        default:
            break;
    }

    return obj;
}

//取得所有拌粉操作人員名單
export async function getExtrusionOperator(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT CNAME, JOB
            FROM PBTC_IOT_EXTRUSION_CREW
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getExtrusionOperator', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得交接紀錄停機原因
export async function getHandoverReason(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT REASON
            FROM PBTC_IOT_HANDOVER_REASON
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY REASON_ID`;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getHandoverReason', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得日產量報表
export async function getDailyForm(date, workShift, type, user) {
    let obj = {
        res: [],
        sumDayRows: [],
        exist: true,
        handoverNote: '',
        waterDeionized: 0,
        waterWaste: 0,
        air: 0,
        error: false,
    };

    const workShiftTime = getWorkShiftTime(date, workShift, true);
    if (workShiftTime.endTime > new Date()) {
        obj.res = '尚未到班別結束前30分鐘';
        obj.error = true;
        return obj;
    }

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得一整天的生產日報
        if ('*' === workShift) {
            sql = `
                SELECT * 
                FROM PBTC_IOT_DAILY_REPORT
                WHERE REPORT_DATE = :REPORT_DATE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                ORDER BY LINE,
                CASE
                    WHEN WORK_SHIFT = '早' THEN 1
                    WHEN WORK_SHIFT = '中' THEN 2
                    WHEN WORK_SHIFT = '晚' THEN 3
                    ELSE 4
                END `;
            params = {
                REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            let dailyResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (!dailyResult.rows.length) {
                throw new Error('未找到相符的生產紀錄');
            }
            obj.res = dailyResult.rows;

            //取得整天累計的加總
            sql = `
                SELECT 
                    LINE, 
                    SUM(WT_PER_SHIFT) AS TOTAL_WT, 
                    SUM(PRODUCTIVITY) AS TOTAL_WT_ACT,
                    100 * SUM(PRODUCTIVITY) / NULLIF(SUM(WT_PER_SHIFT), 0) AS AVABILITY_WT,
                    100 * SUM(PRODUCTION_TIME) / 24 AS AVABILITY_TIME,
                    SUM(STOP_TIME) AS TOTAL_STOP_TIME,
                    SUM(WEIGHT_SCRAP) AS TOTAL_SCRAP,
                    SUM(WEIGHT_SCRAP) / NULLIF((SUM(PRODUCTIVITY) * 1000), 0) AS TOTAL_RATIO_SCRAP,
                    SUM(WEIGHT_HEAD) AS TOTAL_HEAD,
                    SUM(WEIGHT_HEAD) / NULLIF((SUM(PRODUCTIVITY) * 1000), 0) AS TOTAL_RATIO_HEAD,
                    COUNT(*) AS COUNT
                FROM PBTC_IOT_DAILY_REPORT
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND REPORT_DATE = :REPORT_DATE
                GROUP BY LINE `;
            let sumDayResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            obj.sumDayRows = sumDayResult.rows;

            //取得整天的交接事項
            sql = `
                SELECT 
                    SUM(WATER_DEIONIZED) AS WATER_DEIONIZED,
                    SUM(WATER_WASTE) AS WATER_WASTE,
                    SUM(AIR) AS AIR,
                    LISTAGG( NOTE, ',' ) WITHIN GROUP ( ORDER BY 
                        CASE
                            WHEN WORK_SHIFT = '早' THEN 1
                            WHEN WORK_SHIFT = '中' THEN 2
                            WHEN WORK_SHIFT = '晚' THEN 3
                            ELSE 4
                        END ) AS NOTE
                FROM PBTC_IOT_DAILY_HANDOVER
                WHERE REPORT_DATE = :REPORT_DATE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                GROUP BY REPORT_DATE `;
            params = {
                REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            let handoverResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (handoverResult.rows.length) {
                obj.handoverNote = handoverResult.rows[0].NOTE;
                obj.waterDeionized = handoverResult.rows[0].WATER_DEIONIZED;
                obj.waterWaste = handoverResult.rows[0].WATER_WASTE;
                obj.air = handoverResult.rows[0].AIR;
            }

            return obj;
        }

        if ('query' === type) {
            //查詢是否已經建立過該班別的生產日報
            sql = `
                SELECT * 
                FROM PBTC_IOT_DAILY_REPORT
                WHERE REPORT_DATE = :REPORT_DATE
                AND WORK_SHIFT = :WORK_SHIFT
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM`;
            params = {
                REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            let dailyResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (dailyResult.rows.length) {
                obj.res = dailyResult.rows;

                //取得交接事項
                sql = `
                    SELECT NOTE, WATER_DEIONIZED, WATER_WASTE, AIR
                    FROM PBTC_IOT_DAILY_HANDOVER
                    WHERE REPORT_DATE = :REPORT_DATE
                    AND WORK_SHIFT = :WORK_SHIFT
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM `;
                params = {
                    REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                    WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                let handoverResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                if (handoverResult.rows.length) {
                    obj.handoverNote = handoverResult.rows[0].NOTE;
                    obj.waterDeionized = handoverResult.rows[0].WATER_DEIONIZED;
                    obj.waterWaste = handoverResult.rows[0].WATER_WASTE;
                    obj.air = handoverResult.rows[0].AIR;
                }

                return obj;
            }
        }

        obj.exist = false;

        //查詢已建立的交接紀錄表
        sql = `
            SELECT
                HF.LINE, HF.WORK_SHIFT, HF.SEQ, HF.PRD_PC, HF.IC_NAME,
                HF.PRODUCTION_TIME, HF.PRODUCTIVITY, PS.WT_PER_HR
            FROM PBTC_IOT_HANDOVER_FORM HF LEFT JOIN PRO_SCHEDULE PS
                ON HF.LINE = PS.LINE
                AND HF.SEQ = PS.SEQ
                AND HF.COMPANY = PS.COMPANY
                AND HF.FIRM = PS.FIRM
            WHERE HF.RECORD_DATE = :RECORD_DATE --YYYYMMDD
            AND HF.WORK_SHIFT = :WORK_SHIFT
            AND HF.COMPANY = :COMPANY
            AND HF.FIRM = :FIRM
            ORDER BY HF.LINE `;
        params = {
            RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const workResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!workResult.rows.length) {
            throw new Error('尚未建立交接紀錄表');
        }
        //取得領班名字
        let inChargeName = '';
        const inChargeResult = workResult.rows.filter(x => (x.LINE === '領班'));
        if (inChargeResult.length) {
            inChargeName = inChargeResult[0].IC_NAME;
        }

        //取得該班別的抄表用電量
        const lastWorkShift = getLastWorkShift(date, workShift);
        sql = `
            SELECT METER_LINE, SUM(ACTUAL_VALUE) AS ACTUAL_VALUE
            FROM (
                SELECT METER_NO, METER_LINE, MULTIPLE * (MAX(METER_VALUE) - MIN(METER_VALUE)) AS ACTUAL_VALUE
                FROM PBTC_IOT_METER_RECORD 
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND ( RECORD_DATE = :RECORD_DATE AND WORK_SHIFT = :WORK_SHIFT )
                OR ( RECORD_DATE = :LAST_RECORD_DATE AND WORK_SHIFT = :LAST_WORK_SHIFT )
                GROUP BY METER_NO, MULTIPLE, METER_LINE
            )
            GROUP BY METER_LINE `;
        params = {
            RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift },
            LAST_RECORD_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: lastWorkShift.date },
            LAST_WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: lastWorkShift.workShift },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const powerResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (powerResult.rows.length) {
            obj.waterDeionized = powerResult.rows.filter(x => '純水' === x.METER_LINE)[0].ACTUAL_VALUE;
            obj.waterWaste = powerResult.rows.filter(x => '廢水' === x.METER_LINE)[0].ACTUAL_VALUE;
            obj.air = powerResult.rows.filter(x => 'AIR' === x.METER_LINE)[0].ACTUAL_VALUE;
        }

        //查詢此班有在生產中的排程時間
        sql = `
            SELECT LINE, SEQ, PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME, WT_PER_HR
            FROM PRO_SCHEDULE
            WHERE ( 
                ( ACT_END_TIME > :WORK_SHIFT_END_TIME )
                OR ( ACT_END_TIME IS NULL ) 
                OR ( ACT_END_TIME > :WORK_SHIFT_START_TIME AND ACT_END_TIME <= :WORK_SHIFT_END_TIME ) )
            AND ACT_STR_TIME < :WORK_SHIFT_END_TIME
            AND ACT_STR_TIME > TO_DATE('20221001', 'YYYYMMDD') --有一些奇怪的排程從2016到現在還沒結束?
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY LINE, SEQ `;
        params = {
            WORK_SHIFT_START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.startTime) },
            WORK_SHIFT_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.endTime) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        //找出所有線別，再去對應線別找押出"當下狀態"、生產規格、經時、押時、SILO、已生產量、未包裝量
        sql = 'SELECT LINE FROM PBTC_IOT_FEEDER_INFO WHERE COMPANY = :COMPANY AND FIRM = :FIRM GROUP BY LINE';
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const lineResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        let appendArray = [];
        for (const line of lineResult.rows) {
            //取得最新的電表讀數，08/30改抓抄表的電量與上一班的電量差
            //const ammeter = await VisionTagsAPI.getLineAmmeter(line.LINE, user);
            const powerConsumption = powerResult.rows.filter(x => (x.METER_LINE === line.LINE));

            //找出該線別的主控
            const lineWorks = workResult.rows.filter(x => (x.LINE === line.LINE));

            //該線別班別時間內有哪些工令正在生產
            const lineSchedules = scheduleResult.rows.filter(x => (x.LINE === line.LINE));

            if (lineSchedules.length) {
                let lastOrderEndTime = null; //上一個工令的結束時間，要作為下一筆工令開始時間，以利計算生產停車的時間
                for (const schedule of lineSchedules) {
                    const scheduleStartTime = lastOrderEndTime ? lastOrderEndTime : workShiftTime.startTime;
                    const scheduleEndTime = (!schedule.ACT_END_TIME || schedule.ACT_END_TIME > workShiftTime.endTime || 1 === lineSchedules.length) ? workShiftTime.endTime : schedule.ACT_END_TIME;

                    //該工令的生產經時
                    const productionTime = await VisionTagsAPI.getProductionTime(line.LINE, scheduleStartTime, scheduleEndTime, user);

                    //該工令的實際產量
                    const feederResult = await storageDB.getInvtPay('time', null, null, scheduleStartTime, scheduleEndTime, schedule.LOT_NO, user, false, false);

                    //該工令的料頭與前料量查詢
                    sql = `
                        SELECT
                            SUM(WEIGHT_RESTART) AS WEIGHT_RESTART,
                            SUM(WEIGHT_BREAK) AS WEIGHT_BREAK,
                            SUM(WEIGHT_ABNORMAL) AS WEIGHT_ABNORMAL
                        FROM PBTC_IOT_EXTR_SCRAP
                        WHERE LINE = :LINE
                        AND SEQUENCE = :SEQUENCE
                        AND CREATE_TIME > :WORK_SHIFT_START_TIME
                        AND CREATE_TIME <= :WORK_SHIFT_END_TIME
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        GROUP BY LINE, SEQUENCE, COMPANY, FIRM `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.LINE },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: schedule.SEQ },
                        WORK_SHIFT_START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.startTime) },
                        WORK_SHIFT_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.endTime) },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    const scrapResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

                    sql = `
                        SELECT SUM(WEIGHT) AS WEIGHT_HEAD
                        FROM PBTC_IOT_EXTR_HEAD
                        WHERE LINE = :LINE
                        AND SEQUENCE = :SEQUENCE
                        AND CREATE_TIME > :WORK_SHIFT_START_TIME
                        AND CREATE_TIME <= :WORK_SHIFT_END_TIME
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        GROUP BY LINE, SEQUENCE, COMPANY, FIRM `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.LINE },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: schedule.SEQ },
                        WORK_SHIFT_START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.startTime) },
                        WORK_SHIFT_END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(workShiftTime.endTime) },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    };
                    const headResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

                    appendArray.push({
                        LINE: line.LINE,
                        WORK_SHIFT: workShift,
                        SEQ: schedule.SEQ,
                        PRD_PC: schedule.PRD_PC,
                        WT_PER_HR: schedule.WT_PER_HR,
                        WT_PER_SHIFT: schedule.WT_PER_HR * (scheduleEndTime - scheduleStartTime) / (60 * 60 * 1000 * 1000), //每小時產量(MT)*生產時數
                        PRODUCTION_TIME: productionTime.productionTime,
                        STOP_TIME: productionTime.stopTime,
                        PRODUCTIVITY: feederResult.res.length ? feederResult.res[0].FEED_STORAGE / 1000 : 0, //單位MT
                        IC_NAME: inChargeName,
                        CONTROLLER_NAME: lineWorks[0].IC_NAME,
                        WEIGHT_RESTART: scrapResult.rows.length ? scrapResult.rows[0].WEIGHT_RESTART : 0,
                        WEIGHT_BREAK: scrapResult.rows.length ? scrapResult.rows[0].WEIGHT_BREAK : 0,
                        WEIGHT_ABNORMAL: scrapResult.rows.length ? scrapResult.rows[0].WEIGHT_ABNORMAL : 0,
                        WEIGHT_HEAD: headResult.rows.length ? headResult.rows[0].WEIGHT_HEAD : 0,
                        AMMETER: powerConsumption.length ? powerConsumption[0].ACTUAL_VALUE : 0,
                    });

                    lastOrderEndTime = schedule.ACT_END_TIME;
                }

            } else {
                //該線目前並無在生產
                appendArray.push({
                    LINE: line.LINE,
                    WORK_SHIFT: workShift,
                    SEQ: 0,
                    WT_PER_HR: 0,
                    WT_PER_SHIFT: 0,
                    PRODUCTION_TIME: 0,
                    STOP_TIME: 8,
                    PRODUCTIVITY: 0,
                    IC_NAME: inChargeName,
                    CONTROLLER_NAME: lineWorks[0].IC_NAME,
                    WEIGHT_RESTART: 0,
                    WEIGHT_BREAK: 0,
                    WEIGHT_ABNORMAL: 0,
                    WEIGHT_HEAD: 0,
                    AMMETER: powerConsumption.length ? powerConsumption[0].ACTUAL_VALUE : 0,
                });
            }
        }

        obj.res = appendArray;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getDailyForm', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存一個班別的日產能報表
export async function saveDailyForm(date, workShift, formArray, handover, waterDeionized, waterWaste, air, getDataTime, user) {
    let obj = {
        res: null,
        error: false,
    };

    const workShiftTime = getWorkShiftTime(date, workShift);
    if (!moment(new Date()).isBetween(moment(workShiftTime.endTime).subtract(30, 'minutes'), moment(workShiftTime.endTime).add(1, 'hour'))) {
        obj.res = '已超出交接時間，將無法儲存';
        obj.error = true;
        return obj;
    }

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (const row of formArray) {
            const totalScrap = row.WEIGHT_RESTART + row.WEIGHT_BREAK + row.WEIGHT_ABNORMAL;
            sql = `
                BEGIN
                    INSERT INTO PBTC_IOT_DAILY_REPORT (
                        REPORT_DATE, WORK_SHIFT, LINE, SEQ,
                        PRD_PC, WT_PER_HR, WT_PER_HR_ACT, PRODUCTION_TIME, WT_PER_SHIFT, PRODUCTIVITY,
                        DISCONNECT_TIME, STOP_TIME, STOP_1, STOP_2, STOP_3, STOP_4, STOP_5, STOP_6, STOP_7,
                        IC_NAME, CONTROLLER_NAME, WEIGHT_RESTART, WEIGHT_BREAK, WEIGHT_ABNORMAL, WEIGHT_SCRAP, WEIGHT_HEAD,
                        AMMETER, GET_DATA_TIME, NOTE,
                        COMPANY, FIRM, DEPT )
                    VALUES (
                        :REPORT_DATE, :WORK_SHIFT, :LINE, :SEQ,
                        :PRD_PC, :WT_PER_HR, :WT_PER_HR_ACT, :PRODUCTION_TIME, :WT_PER_SHIFT, :PRODUCTIVITY,
                        :DISCONNECT_TIME, :STOP_TIME, :STOP_1, :STOP_2, :STOP_3, :STOP_4, :STOP_5, :STOP_6, :STOP_7,
                        :IC_NAME, :CONTROLLER_NAME, :WEIGHT_RESTART, :WEIGHT_BREAK, :WEIGHT_ABNORMAL, :WEIGHT_SCRAP, :WEIGHT_HEAD,
                        :AMMETER, :GET_DATA_TIME, :NOTE,
                        :COMPANY, :FIRM, :DEPT );
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE PBTC_IOT_DAILY_REPORT
                        SET WT_PER_HR_ACT =:WT_PER_HR_ACT,
                            PRODUCTION_TIME = :PRODUCTION_TIME,
                            PRODUCTIVITY = :PRODUCTIVITY,
                            DISCONNECT_TIME = :DISCONNECT_TIME,
                            STOP_TIME = :STOP_TIME,
                            STOP_1 = :STOP_1,
                            STOP_2 = :STOP_2,
                            STOP_3 = :STOP_3,
                            STOP_4 = :STOP_4,
                            STOP_5 = :STOP_5,
                            STOP_6 = :STOP_6,
                            STOP_7 = :STOP_7,
                            WEIGHT_RESTART = :WEIGHT_RESTART,
                            WEIGHT_BREAK = :WEIGHT_BREAK,
                            WEIGHT_ABNORMAL = :WEIGHT_ABNORMAL,
                            WEIGHT_SCRAP = :WEIGHT_SCRAP,
                            WEIGHT_HEAD = :WEIGHT_HEAD,
                            AMMETER = :AMMETER,
                            NOTE = :NOTE
                        WHERE REPORT_DATE = :REPORT_DATE
                        AND WORK_SHIFT = :WORK_SHIFT
                        AND LINE = :LINE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM
                        AND DEPT = :DEPT;
                END; `;
            params = {
                REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
                WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LINE.toString() },
                SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.SEQ) },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC || '' },
                WT_PER_HR: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WT_PER_HR) },
                WT_PER_HR_ACT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(0 < row.WT_PER_HR_ACT ? row.WT_PER_HR_ACT : 0) },
                PRODUCTION_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.PRODUCTION_TIME) },
                WT_PER_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WT_PER_SHIFT) },
                PRODUCTIVITY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(0 < row.PRODUCTIVITY ? row.PRODUCTIVITY : 0) },
                DISCONNECT_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.DISCONNECT_TIME) },
                STOP_TIME: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_TIME) },
                STOP_1: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_1 || 0) },
                STOP_2: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_2 || 0) },
                STOP_3: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_3 || 0) },
                STOP_4: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_4 || 0) },
                STOP_5: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_5 || 0) },
                STOP_6: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_6 || 0) },
                STOP_7: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.STOP_7 || 0) },
                IC_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.IC_NAME },
                CONTROLLER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.CONTROLLER_NAME },
                WEIGHT_RESTART: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WEIGHT_RESTART) },
                WEIGHT_BREAK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WEIGHT_BREAK) },
                WEIGHT_ABNORMAL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WEIGHT_ABNORMAL) },
                WEIGHT_SCRAP: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(totalScrap) },
                WEIGHT_HEAD: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.WEIGHT_HEAD) },
                AMMETER: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.AMMETER) },
                GET_DATA_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: moment(getDataTime).toDate() },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOTE || '' },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            };
            await conn.execute(sql, params, { autoCommit: true });
        }

        //儲存交接事項欄位
        sql = `
            BEGIN
                INSERT INTO PBTC_IOT_DAILY_HANDOVER (
                    REPORT_DATE, WORK_SHIFT, NOTE,
                    WATER_DEIONIZED, WATER_WASTE, AIR,
                    COMPANY, FIRM, DEPT )
                VALUES (
                    :REPORT_DATE, :WORK_SHIFT, :NOTE,
                    :WATER_DEIONIZED, :WATER_WASTE, :AIR,
                    :COMPANY, :FIRM, :DEPT );
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE PBTC_IOT_DAILY_HANDOVER
                    SET NOTE = :NOTE,
                        WATER_DEIONIZED = :WATER_DEIONIZED,
                        WATER_WASTE = :WATER_WASTE,
                        AIR = :AIR
                    WHERE REPORT_DATE = :REPORT_DATE
                    AND WORK_SHIFT = :WORK_SHIFT
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT;
            END; `;
        params = {
            REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: handover || '' },
            WATER_DEIONIZED: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(waterDeionized) },
            WATER_WASTE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(waterWaste) },
            AIR: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(air) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        };
        await conn.execute(sql, params, { autoCommit: true });

        //處理完的再寄信
        sql = `
            SELECT *
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE = :REPORT_DATE
            AND WORK_SHIFT = :WORK_SHIFT
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY LINE`;
        params = {
            REPORT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: date.toString() },
            WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: workShift.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let formResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (formResult.rows.length) {
            Mailer.saveDailyForm(date, workShift, formResult.rows, handover, user);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'saveDailyForm', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查時間區間內各線別的產量/經時稼動率
export async function getLineAvability(month, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT 
                REPORT_DATE, LINE,
                SUM(NULLIF(PRODUCTIVITY, 0)) * 100 / SUM(NULLIF(WT_PER_SHIFT, 0)) AS AVABILITY_WT,
                SUM(PRODUCTION_TIME) * 100 / 24 AS AVABILITY_TIME
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE LIKE :MONTH
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY REPORT_DATE, LINE, COMPANY, FIRM
            ORDER BY REPORT_DATE, LINE `;
        let params = {
            MONTH: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: month + '%' },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getLineAvability', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查時間區間內每個領班/人員的個人績效
export async function getCrewPerformance(startDate, endDate, user) {
    let obj = {
        ic: [],
        controller: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT 
                IC_NAME,
                SUM(PRODUCTIVITY) AS PRODUCTIVITY,
                SUM(WEIGHT_RESTART) + SUM(WEIGHT_BREAK) + SUM(WEIGHT_ABNORMAL) AS SCRAP_WEIGHT,
                SUM(PRODUCTION_TIME) AS PRODUCTION_TIME,
                SUM(STOP_1) + SUM(STOP_2) + SUM(STOP_4) AS STOP_TIME
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE < :END_DATE
            AND REPORT_DATE >= :START_DATE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY IC_NAME
            ORDER BY PRODUCTIVITY DESC `;
        params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const IcResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.ic = IcResult.rows;

        sql = `
            SELECT 
                CONTROLLER_NAME,
                SUM(PRODUCTIVITY) AS PRODUCTIVITY,
                SUM(WEIGHT_RESTART) + SUM(WEIGHT_BREAK) + SUM(WEIGHT_ABNORMAL) AS SCRAP_WEIGHT,
                SUM(PRODUCTION_TIME) AS PRODUCTION_TIME,
                SUM(STOP_1) + SUM(STOP_2) + SUM(STOP_4) AS STOP_TIME
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE < :END_DATE
            AND REPORT_DATE >= :START_DATE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND CONTROLLER_NAME != '缺員'
            GROUP BY CONTROLLER_NAME
            ORDER BY PRODUCTIVITY DESC `;
        params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const controllerResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.controller = controllerResult.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getCrewPerformance', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查時間區間內的生產總表
export async function getProductionSummary(month, user) {
    let obj = {
        res: [],
        goal: 0,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT
                REPORT_DATE, LINE,
                SUM(PRODUCTIVITY) AS PRODUCTIVITY,
                SUM(WEIGHT_SCRAP) / 1000 AS WEIGHT_SCRAP
            FROM PBTC_IOT_DAILY_REPORT
            WHERE REPORT_DATE LIKE :MONTH
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY REPORT_DATE, LINE
            ORDER BY REPORT_DATE, LINE `;
        params = {
            MONTH: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: month + '%' },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;

        //取得銷貨目標
        sql = `
            SELECT
                SUM(BU_SDNEW.QTY) AS QTY_T,
                DECODE(BU_SDNEW.SALE,'D',SUM(BU_SDNEW.QTY),0) AS QTY_TD,
                DECODE(BU_SDNEW.SALE,'E',SUM(BU_SDNEW.QTY),0) AS QTY_TE
            FROM BU_SDNEW
            WHERE (BU_SDNEW.QTY > 0 OR BU_SDNEW.UPR > 0 OR BU_SDNEW.FUPR > 0 OR BU_SDNEW.AMT_TWD > 0)
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND LEVEL_1 = 'PBT'
            AND YYYYMM = :MONTH
            GROUP BY BU_SDNEW.COMPANY, BU_SDNEW.FIRM, BU_SDNEW.LEVEL_1, BU_SDNEW.SALE, BU_SDNEW.UNIT
            ORDER BY BU_SDNEW.COMPANY, BU_SDNEW.FIRM, BU_SDNEW.LEVEL_1, BU_SDNEW.SALE, BU_SDNEW.UNIT `;
        params = {
            MONTH: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: month },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (2 === result.rows.length) {
            obj.goal = result.rows[0].QTY_T + result.rows[1].QTY_T;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getProductionSummary', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import moment from 'moment';
import oracledb from 'oracledb';


//查詢包裝日報表
export async function getPackingDailyReport(user, packingDateStart, packingDateEnd) {
    const obj = {
        attendanceReport: [],
        detailReport: [],
        error: null,
    };

    //額外條件
    if (!packingDateEnd) {
        packingDateEnd = packingDateStart;
    }

    let conn;
    try {
        if (!user || !user.COMPANY) {
            throw new Error('無法判斷使用者身分，請重新登入');
        }
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        obj.attendanceReport = await getPackingDailyAttendanceReport(conn, user, packingDateStart, packingDateEnd);
        obj.detailReport = await getPackingDailyDetailReport(conn, user, packingDateStart, packingDateEnd);
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingDailyReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//包裝日報表 - 每日包裝出勤及槽車灌充作業
async function getPackingDailyAttendanceReport(conn, user, packingDateStart, packingDateEnd) {
    const selectSQL = `
    SELECT S0.COMPANY, S0.FIRM, S0.DEPT, 
        S0.PACKING_DATE, S0.PACKING_SHIFT, S0.FOREMAN1, 
        S0.FOREMAN2, S0.AUTO_PACKING_HEADCOUNT, S0.MANUAL_PACKING_HEADCOUNT, 
        S0.OVERTIME_HEADCOUNT, 
        S0.FILLING_QUANTITY2, S0.FILLING_QUANTITY1, S0.NOTE, 
        S0.SEA_BULK_TANK, S0.FIBC_FILLING_TANK, S0.PACKING_MATERIAL_CARRY, 
        S0.OFF_LINE_BAG_PRINT, S0.BAG_ATTACH_LABEL, 
        S0.BAG_STAMP, S0.BAG_RESTACK
    FROM AC.PBTC_IOT_PACK_ATTENDANCE_RPT S0  LEFT JOIN AC.PBTC_IOT_PACKING_SHIFT S1
        ON S0.COMPANY = S1.COMPANY
            AND S0.FIRM = S1.FIRM
            AND S0.PACKING_SHIFT = S1.SHIFT_NAME
    WHERE 1 = 1
        AND S0.COMPANY = :COMPANY
        AND S0.FIRM = :FIRM 
        AND TRUNC(S0.PACKING_DATE) BETWEEN TO_DATE(:PACKING_DATE_START, 'YYYY-MM-DD') AND TO_DATE(:PACKING_DATE_END, 'YYYY-MM-DD') 
    ORDER BY S0.PACKING_DATE ASC, S1.SHIFT_ORDER ASC
    `;
    const insertSQL = `
    INSERT INTO AC.PBTC_IOT_PACK_ATTENDANCE_RPT (
        COMPANY, FIRM, DEPT, 
        PACKING_DATE, PACKING_SHIFT, 
        CREATE_USER_NAME, CREATE_USER ) 
    VALUES (
        :COMPANY, :FIRM, :DEPT, 
        TO_DATE(:PACKING_DATE, 'YYYY-MM-DD'), :PACKING_SHIFT, 
        :CREATE_USER_NAME, :CREATE_USER
    )
    `;

    const startDate = new Date(packingDateStart);
    const endDate = new Date(packingDateEnd);
    const days = ~~((endDate.getTime() - startDate.getTime()) / 86400000) + 1;

    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        PACKING_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart },
        PACKING_DATE_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateEnd },
    };
    const options = { outFormat: oracledb.OBJECT };

    let result = await conn.execute(selectSQL, params, options);

    //檢查班別報表資料是否缺少
    const shifts = ['早', '中', '夜'];
    if (!result.rows.length || result.rows.length < days * shifts.length) {
        //找出查詢結果中有哪些日期
        const packingDates = new Set();
        for (let i = 0; i < result.rows.length; i++) {
            const row = result.rows[i];
            packingDates.add(row.PACKING_DATE ? row.PACKING_DATE.getTime() : 0);
        }
        //console.log(packingDates);
        //找出尚未建立資料的日期，並新增資料
        let needCommit = false;
        for (let i = 0; i < days; i++) {
            const date = startDate.getTime() + startDate.getTimezoneOffset() * 60000 + i * 86400000;
            if (packingDates.has(date)) {
                continue;
            }
            for (let shiftIndex = 0; shiftIndex < shifts.length; shiftIndex++) {
                const insertParams = {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                    PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(date).format('YYYY-MM-DD') },
                    PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: shifts[shiftIndex] },
                    CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                    CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                };
                // console.log(insertSQL, insertParams);
                try {
                    await conn.execute(insertSQL, insertParams, options);
                } catch (err) {
                    console.error(getNowDatetimeString(), 'getPackingDailyAttendanceReport', insertParams);
                    throw err;
                }
            }
            needCommit = true;
        }
        if (needCommit) {
            await conn.commit();
        }

        //新增完後重新查詢
        result = await conn.execute(selectSQL, params, options);
    }

    return result.rows;
}

//包裝日報表 - 每日包裝明細表
async function getPackingDailyDetailReport(conn, user, packingDateStart, packingDateEnd) {
    const selectSQL = `
        WITH PACKING_DETAIL AS (
            SELECT S0.COMPANY, S0.FIRM, S0.DEPT, 
                S0.PACKING_SEQ, 
                DECODE(TRUNC(TO_CHAR(S0.CONFIRM_TIME, 'HH24') / 8), 0, '夜', 1, '早', '中') AS PACKING_SHIFT,
                S1.PACKING_LINE, 
                S2.LINE_NAME, S1.PRD_PC, S1.LOT_NO, S1.SILO_NO, 
                S1.TARGET_WEIGHT, S1.PACKING_MATERIAL, S1.PACKING_MATERIAL_ID, S1.PACKING_WEIGHT_SPEC, 
                (GREATEST((S0.DETAIL_SEQ_END - S0.DETAIL_SEQ_START - S0.SEQ_ERROR_COUNT + 1), 0) * S1.PACKING_WEIGHT_SPEC) AS PACKING_WEIGHT, 
                S0.CONFIRM_TIME, 
                (SELECT MAX(CONFIRM_TIME) FROM AC.PBTC_IOT_PACKING_DETAIL WHERE COMPANY = S0.COMPANY AND FIRM = S0.FIRM AND PACKING_SEQ = S0.PACKING_SEQ) AS LAST_CONFIRM_TIME,
                S1.REMAINDER_WEIGHT, 
                S2.STACKING_METHOD AS DEFAULT_STACKING_METHOD,
                DECODE(S2.LINE_TYPE, 'AUTO', '自動', '自動', '自動', '手動') AS DEFAULT_PACKING_METHOD,
                TRUNC(S0.CONFIRM_TIME - 8/24) AS PACKING_DATE
            FROM PBTC_IOT_PACKING_DETAIL S0 JOIN AC.PBTC_IOT_PACKING_SCHEDULE S1
                ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.PACKING_SEQ = S1.PACKING_SEQ
                JOIN AC.PBTC_IOT_PACKING_LINE S2
                    ON S1.COMPANY = S2.COMPANY
                        AND S1.FIRM = S2.FIRM
                        AND S1.PACKING_LINE = S2.LINE_ID
            WHERE 1 = 1
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM 
                AND S0.CONFIRM_TIME BETWEEN (TO_DATE(:PACKING_DATE_START, 'YYYY-MM-DD') + 8/24) AND (TO_DATE(:PACKING_DATE_END, 'YYYY-MM-DD') + 32/24) 
            ORDER BY S0.PACKING_SEQ ASC
        )
        SELECT S0.*, 
            S1.CREATE_TIME AS REPORT_CREATE_TIME, S1.STACKING_METHOD, S1.PACKING_METHOD, 
            S1.ABNORMAL_REASON1, S1.ABNORMAL_TIME1, S1.ABNORMAL_REASON2, S1.ABNORMAL_TIME2, 
            S1.CLEAN_TIMES, S1.CLEAN_TIMES2, S1.CLEAN_MINUTES, S1.NOTE 
        FROM (
            SELECT COMPANY, FIRM, MAX(DEPT) AS DEPT, PACKING_SEQ, PACKING_SHIFT, 
                MAX(PACKING_LINE) AS PACKING_LINE, 
                MAX(LINE_NAME) AS LINE_NAME, 
                MAX(PRD_PC) AS PRD_PC, MAX(LOT_NO) AS LOT_NO, 
                MAX(SILO_NO) AS SILO_NO, 
                MAX(TARGET_WEIGHT) AS TARGET_WEIGHT, 
                MAX(PACKING_MATERIAL) AS PACKING_MATERIAL, 
                MAX(PACKING_MATERIAL_ID) AS PACKING_MATERIAL_ID, 
                TO_NUMBER(MAX(PACKING_WEIGHT_SPEC)) AS PACKING_WEIGHT_SPEC, 
                SUM(PACKING_WEIGHT) AS PACKING_TOTAL_WEIGHT, 
                COUNT(1) AS PACKING_DETAIL_COUNT, 
                SUM(DECODE(CONFIRM_TIME, LAST_CONFIRM_TIME, REMAINDER_WEIGHT, 0)) AS REMAINDER_WEIGHT, --殘包重量 歸屬到排程的最後一班
                MAX(DEFAULT_STACKING_METHOD) AS DEFAULT_STACKING_METHOD, 
                MAX(DEFAULT_PACKING_METHOD) AS DEFAULT_PACKING_METHOD, 
                MAX(PACKING_DATE) AS PACKING_DATE,
                MIN(CONFIRM_TIME) AS MIN_CONFIRM_TIME
            FROM PACKING_DETAIL
            GROUP BY COMPANY, FIRM, PACKING_SEQ, PACKING_SHIFT
            ORDER BY PACKING_SEQ ASC, MIN(CONFIRM_TIME) ASC
        ) S0 LEFT JOIN AC.PBTC_IOT_PACK_DETAIL_RPT S1
        ON S0.COMPANY = S1.COMPANY
            AND S0.FIRM = S1.FIRM
            AND S0.PACKING_SEQ = S1.PACKING_SEQ
            AND S0.PACKING_SHIFT = S1.PACKING_SHIFT
        ORDER BY S0.PACKING_DATE ASC, S0.PACKING_SEQ ASC, S0.MIN_CONFIRM_TIME ASC
    `;
    const insertSQL = `
    INSERT INTO AC.PBTC_IOT_PACK_DETAIL_RPT (
        COMPANY, FIRM, DEPT, 
        PACKING_SHIFT, PACKING_SEQ, 
        STACKING_METHOD, PACKING_METHOD, 
        PACKING_DATE, 
        CREATE_USER_NAME, CREATE_USER
    ) 
    VALUES (
        :COMPANY, :FIRM, :DEPT, 
        :PACKING_SHIFT, :PACKING_SEQ, 
        :STACKING_METHOD, :PACKING_METHOD, 
        :PACKING_DATE, 
        :CREATE_USER_NAME, :CREATE_USER
    )
    `;

    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        PACKING_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart },
        PACKING_DATE_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateEnd },
    };
    const options = { outFormat: oracledb.OBJECT };

    let result = await conn.execute(selectSQL, params, options);
    // console.log(params, result);

    //自動產生報表資料
    let insertCount = 0;
    for (let i = 0; i < result.rows.length; i++) {
        const row = result.rows[i];
        if (!row.REPORT_CREATE_TIME) {
            insertCount++;
            //包裝方式
            let packingMethod = row.DEFAULT_PACKING_METHOD;
            //例外條件，高雄廠 手動C線+太空袋 => 包裝方式預設為空
            if ('7' === row.FIRM && '手動C線' === row.LINE_NAME && 'T' === row.PACKING_MATERIAL_ID[0]) {
                packingMethod = null;
            }
            const insertParams = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                STACKING_METHOD: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEFAULT_STACKING_METHOD },
                PACKING_METHOD: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingMethod },
                PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PACKING_DATE },
                CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
            };

            // console.log(insertSQL, insertParams);
            try {
                await conn.execute(insertSQL, insertParams, options);
            } catch (err) {
                console.error(getNowDatetimeString(), 'getPackingDailyDetailReport', insertParams);
                throw err;
            }
        }
    }
    if (insertCount) {
        await conn.commit();

        //新增完後重新查詢
        result = await conn.execute(selectSQL, params, options);
    }

    return result.rows;
}

//包裝日報表 - 儲存 每日包裝出勤及槽車灌充作業
export async function savePackingDailyAttendanceReport(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const mergeSQL = `
        MERGE INTO AC.PBTC_IOT_PACK_ATTENDANCE_RPT USING DUAL ON (
            COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PACKING_DATE = TRUNC(:PACKING_DATE)
                AND PACKING_SHIFT = :PACKING_SHIFT
        )
        WHEN MATCHED THEN 
            UPDATE SET 
                FOREMAN1                 = :FOREMAN1,
                FOREMAN2                 = :FOREMAN2,
                AUTO_PACKING_HEADCOUNT   = :AUTO_PACKING_HEADCOUNT,
                MANUAL_PACKING_HEADCOUNT = :MANUAL_PACKING_HEADCOUNT,
                OVERTIME_HEADCOUNT       = :OVERTIME_HEADCOUNT,
                FILLING_QUANTITY1        = :FILLING_QUANTITY1,
                FILLING_QUANTITY2        = :FILLING_QUANTITY2,
                NOTE                     = :NOTE,
                SEA_BULK_TANK            = :SEA_BULK_TANK,
                FIBC_FILLING_TANK        = :FIBC_FILLING_TANK,
                PACKING_MATERIAL_CARRY   = :PACKING_MATERIAL_CARRY,
                OFF_LINE_BAG_PRINT       = :OFF_LINE_BAG_PRINT,
                BAG_ATTACH_LABEL         = :BAG_ATTACH_LABEL,
                BAG_STAMP                = :BAG_STAMP,
                BAG_RESTACK              = :BAG_RESTACK,
                EDIT_USER_NAME           = :EDIT_USER_NAME,
                EDIT_USER                = :EDIT_USER,
                EDIT_TIME                = SYSDATE
        WHEN NOT MATCHED THEN 
            INSERT (
                COMPANY, FIRM, DEPT, 
                PACKING_DATE, PACKING_SHIFT, 
                FOREMAN1, FOREMAN2, 
                AUTO_PACKING_HEADCOUNT, MANUAL_PACKING_HEADCOUNT, 
                OVERTIME_HEADCOUNT, 
                FILLING_QUANTITY1, FILLING_QUANTITY2, NOTE, 
                SEA_BULK_TANK, FIBC_FILLING_TANK, PACKING_MATERIAL_CARRY, 
                OFF_LINE_BAG_PRINT, BAG_ATTACH_LABEL, 
                BAG_STAMP, BAG_RESTACK, 
                CREATE_USER_NAME, CREATE_USER, CREATE_TIME
            )
            VALUES (
                :COMPANY, :FIRM, :DEPT, 
                TRUNC(:PACKING_DATE), :PACKING_SHIFT, 
                :FOREMAN1, :FOREMAN2, 
                :AUTO_PACKING_HEADCOUNT, :MANUAL_PACKING_HEADCOUNT, 
                :OVERTIME_HEADCOUNT, 
                :FILLING_QUANTITY1, :FILLING_QUANTITY2, :NOTE, 
                :SEA_BULK_TANK, :FIBC_FILLING_TANK, :PACKING_MATERIAL_CARRY, 
                :OFF_LINE_BAG_PRINT, :BAG_ATTACH_LABEL, 
                :BAG_STAMP, :BAG_RESTACK,
                :CREATE_USER_NAME, :CREATE_USER, SYSDATE
            )
        `;
        const selectSQL = `
        SELECT *
        FROM AC.PBTC_IOT_PACK_ATTENDANCE_RPT
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_DATE = TRUNC(:PACKING_DATE)
            AND PACKING_SHIFT = :PACKING_SHIFT
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            //Number型態轉換
            [
                'AUTO_PACKING_HEADCOUNT', 'MANUAL_PACKING_HEADCOUNT', 'OVERTIME_HEADCOUNT',
                'FILLING_QUANTITY1', 'FILLING_QUANTITY2',
                'SEA_BULK_TANK', 'FIBC_FILLING_TANK', 'PACKING_MATERIAL_CARRY',
                'OFF_LINE_BAG_PRINT', 'BAG_ATTACH_LABEL',
                'BAG_STAMP', 'BAG_RESTACK',
            ].forEach(field => {
                row[field] = (null === row[field]) ? null : Number(row[field]);
                if (isNaN(row[field])) {
                    row[field] = null;
                }
            });

            //Date型態轉換
            ['PACKING_DATE'].forEach(field => {
                if ('string' === typeof row[field]) {
                    row[field] = new Date(row[field]);
                }
            });

            let result = null;
            const params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PACKING_DATE },
                PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                FOREMAN1: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FOREMAN1 },
                FOREMAN2: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FOREMAN2 },
                AUTO_PACKING_HEADCOUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.AUTO_PACKING_HEADCOUNT },
                MANUAL_PACKING_HEADCOUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.MANUAL_PACKING_HEADCOUNT },
                OVERTIME_HEADCOUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.OVERTIME_HEADCOUNT },
                FILLING_QUANTITY1: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.FILLING_QUANTITY1 },
                FILLING_QUANTITY2: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.FILLING_QUANTITY2 },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOTE },
                SEA_BULK_TANK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.SEA_BULK_TANK },
                FIBC_FILLING_TANK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.FIBC_FILLING_TANK },
                PACKING_MATERIAL_CARRY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.PACKING_MATERIAL_CARRY },
                OFF_LINE_BAG_PRINT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.OFF_LINE_BAG_PRINT },
                BAG_ATTACH_LABEL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.BAG_ATTACH_LABEL },
                BAG_STAMP: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.BAG_STAMP },
                BAG_RESTACK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.BAG_RESTACK },
                CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
            };
            result = await conn.execute(mergeSQL, params, options);
            //console.log('merge', result);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'savePackingDailyAttendanceReport 更新失敗');
            }
            if (result && result.lastRowid) {
                result = await conn.execute(selectSQL, {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                    PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PACKING_DATE },
                    PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                }, options);
                //console.log('select', result);
                if (result.rows && result.rows.length) {
                    obj.res.push(result.rows[0]);
                }
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingDailyAttendanceReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//包裝日報表 - 儲存 每日包裝明細表
export async function savePackingDailyDetailReport(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const mergeSQL = `
        MERGE INTO AC.PBTC_IOT_PACK_DETAIL_RPT USING DUAL ON (
            COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PACKING_SHIFT = :PACKING_SHIFT
                AND PACKING_SEQ = :PACKING_SEQ
        )
        WHEN MATCHED THEN 
            UPDATE SET 
                ABNORMAL_REASON1 = :ABNORMAL_REASON1,
                ABNORMAL_TIME1   = :ABNORMAL_TIME1,
                ABNORMAL_REASON2 = :ABNORMAL_REASON2,
                ABNORMAL_TIME2   = :ABNORMAL_TIME2,
                STACKING_METHOD  = :STACKING_METHOD,
                PACKING_METHOD   = :PACKING_METHOD,
                CLEAN_TIMES      = :CLEAN_TIMES,
                CLEAN_TIMES2     = :CLEAN_TIMES2,
                CLEAN_MINUTES    = :CLEAN_MINUTES,
                NOTE             = :NOTE,
                EDIT_USER_NAME   = :EDIT_USER_NAME,
                EDIT_USER        = :EDIT_USER,
                EDIT_TIME        = SYSDATE
        WHEN NOT MATCHED THEN 
            INSERT (
                COMPANY, FIRM, DEPT, 
                PACKING_SHIFT, PACKING_SEQ, 
                ABNORMAL_REASON1, ABNORMAL_TIME1, 
                ABNORMAL_REASON2, ABNORMAL_TIME2, 
                STACKING_METHOD, PACKING_METHOD, 
                CLEAN_TIMES, CLEAN_TIMES2, 
                CLEAN_MINUTES, NOTE, 
                CREATE_USER_NAME, CREATE_USER, CREATE_TIME
            )
            VALUES (
                :COMPANY, :FIRM, :DEPT, 
                :PACKING_SHIFT, :PACKING_SEQ, 
                :ABNORMAL_REASON1, :ABNORMAL_TIME1, 
                :ABNORMAL_REASON2, :ABNORMAL_TIME2, 
                :STACKING_METHOD, :PACKING_METHOD, 
                :CLEAN_TIMES, :CLEAN_TIMES2, 
                :CLEAN_MINUTES, :NOTE, 
                :CREATE_USER_NAME, :CREATE_USER, SYSDATE
            )
        `;
        const selectSQL = `
        SELECT *
        FROM AC.PBTC_IOT_PACK_DETAIL_RPT
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_SHIFT = :PACKING_SHIFT
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            //Number型態轉換
            row.ABNORMAL_TIME1 = (null === row.ABNORMAL_TIME1) ? null : Number(row.ABNORMAL_TIME1);
            row.ABNORMAL_TIME2 = (null === row.ABNORMAL_TIME2) ? null : Number(row.ABNORMAL_TIME2);

            let result = null;
            const params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                ABNORMAL_REASON1: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.ABNORMAL_REASON1 },
                ABNORMAL_TIME1: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.ABNORMAL_TIME1 },
                ABNORMAL_REASON2: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.ABNORMAL_REASON2 },
                ABNORMAL_TIME2: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.ABNORMAL_TIME2 },
                STACKING_METHOD: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.STACKING_METHOD },
                PACKING_METHOD: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_METHOD },
                CLEAN_TIMES: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.CLEAN_TIMES },
                CLEAN_TIMES2: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.CLEAN_TIMES2 },
                CLEAN_MINUTES: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.CLEAN_MINUTES },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOTE },
                CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
            };
            result = await conn.execute(mergeSQL, params, options);
            //console.log('merge', result);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'savePackingDailyDetailReport 更新失敗');
            }
            if (result && result.lastRowid) {
                result = await conn.execute(selectSQL, {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                    PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                    PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                }, options);
                if (result.rows && result.rows.length) {
                    obj.res.push(result.rows[0]);
                }
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingDailyDetailReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢包裝統計表、包裝量統計表
export async function getPackingStatReport(user, packingDateStart, packingDateEnd, isGroupBySchedule) {
    const obj = {
        statReport: [],
        shiftList: [],
        error: null,
    };

    //額外條件
    if (!packingDateEnd) {
        packingDateEnd = packingDateStart;
    }

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const attendanceReport = await getPackingDailyAttendanceReport(conn, user, packingDateStart, packingDateEnd);
        const detailReport = await getPackingDailyDetailReport(conn, user, packingDateStart, packingDateEnd);

        //排除查無資料的情況
        if (!attendanceReport || !attendanceReport.length) {
            return obj;
        }

        //以「每日包裝出勤及槽車灌充作業」為主，產生報表資料
        const reportMap = new Map();
        const shiftList = [];
        const firstPackingDate = attendanceReport[0].PACKING_DATE;
        for (let i = 0; i < attendanceReport.length; i++) {
            const row = attendanceReport[i];
            if (firstPackingDate === row.PACKING_DATE) {
                //取得班別清單
                shiftList.push(row.PACKING_SHIFT); //預計: ['早', '中', '夜']
            }

            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            reportMap.set(key, {
                ...row,
                FOREMAN: `${row.FOREMAN1 ?? ''}${row.FOREMAN2 ? ` ${row.FOREMAN2}` : ''}`,
                AUTO_PACKING_QUANTITY: 0, //自動包裝總量
                MANUAL_PACKING_QUANTITY: 0, //手動包裝總量
                TOTAL_FILLING_QUANTITY: Number(row.FILLING_QUANTITY1) + Number(row.FILLING_QUANTITY2), //槽車灌充量
                TOTAL_PACKING_QUANTITY: 0, //總包裝量(含灌充)
            });
        }
        obj.shiftList = shiftList;

        //包裝統計表: 同一個排程的多筆明細在同一天，只計算最後一個班別 (isGroupBySchedule: true)
        //包裝量統計表: 同一個排程的多筆明細分別計算 (isGroupBySchedule: false)
        const detailMap = new Map();
        detailReport.forEach(row => {
            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.LINE_NAME}_${row.PACKING_SEQ}_${isGroupBySchedule ? '' : row.PACKING_SHIFT}`;
            let detail;
            if (detailMap.has(key)) {
                detail = detailMap.get(key);
            } else {
                detail = {
                    ...row,
                    PACKING_TOTAL_WEIGHT: 0,
                };
                detailMap.set(key, detail);
            }
            detail.PACKING_SHIFT = row.PACKING_SHIFT;
            detail.PACKING_TOTAL_WEIGHT += row.PACKING_TOTAL_WEIGHT;
            //自動產生堆疊方式
            if (!detail.STACKING_METHOD) {
                detail.STACKING_METHOD = detail.LINE_NAME.includes('自動') ? '自動' : '手動';
            }
        });

        //整理報表資料
        for (const row of detailMap.values()) {
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            if (!reportMap.has(key)) {
                console.warn('包裝統計表 找不到key', key, row);
                continue;
            }
            const report = reportMap.get(key);
            if ('自動' === row.DEFAULT_PACKING_METHOD) {
                report.AUTO_PACKING_QUANTITY += row.PACKING_TOTAL_WEIGHT;
            } else {
                report.MANUAL_PACKING_QUANTITY += row.PACKING_TOTAL_WEIGHT;
            }
            report.TOTAL_PACKING_QUANTITY = report.AUTO_PACKING_QUANTITY + report.MANUAL_PACKING_QUANTITY + report.TOTAL_FILLING_QUANTITY;

            report[`${row.LINE_NAME}_TARGET_WEIGHT`] = ~~report[`${row.LINE_NAME}_TARGET_WEIGHT`] + row.TARGET_WEIGHT;
            report[`${row.LINE_NAME}_PACKING_WEIGHT`] = ~~report[`${row.LINE_NAME}_PACKING_WEIGHT`] + row.PACKING_TOTAL_WEIGHT;
            report[`${row.LINE_NAME}_ACHIEVEMENT_RATE`] = report[`${row.LINE_NAME}_TARGET_WEIGHT`] ? (report[`${row.LINE_NAME}_PACKING_WEIGHT`] / report[`${row.LINE_NAME}_TARGET_WEIGHT`]) : null;
        }

        //產生每日小計
        const statReport = [];
        let prevPackingDate = null;
        let sumRow = {
            AUTO_PACKING_HEADCOUNT: 0, //外包自動包裝人數
            MANUAL_PACKING_HEADCOUNT: 0, //外包手動包裝人數
            OVERTIME_HEADCOUNT: 0, //包裝加班人數
            AUTO_PACKING_QUANTITY: 0, //自動包裝總量
            MANUAL_PACKING_QUANTITY: 0, //手動包裝總量
            TOTAL_FILLING_QUANTITY: 0, //槽車灌充量
            TOTAL_PACKING_QUANTITY: 0, //總包裝量(含灌充)
        };
        for (const row of reportMap.values()) {
            if (prevPackingDate !== row.PACKING_DATE) {
                if (statReport.length) {
                    statReport.push({
                        PACKING_DATE: prevPackingDate,
                        PACKING_SHIFT: '小計',
                        ...sumRow,
                    });
                    //清除各包裝線的小計
                    for (let key in sumRow) {
                        if (key.endsWith('_TARGET_WEIGHT') || key.endsWith('_PACKING_WEIGHT')) {
                            const lineName = key.split('_')[0];
                            delete sumRow[`${lineName}_TARGET_WEIGHT`];
                            delete sumRow[`${lineName}_PACKING_WEIGHT`];
                            delete sumRow[`${lineName}_ACHIEVEMENT_RATE`];
                        }
                    }
                }
                for (let key in sumRow) {
                    sumRow[key] = 0;
                }
                prevPackingDate = row.PACKING_DATE;
            }

            sumRow.AUTO_PACKING_HEADCOUNT += Number(row.AUTO_PACKING_HEADCOUNT);
            sumRow.MANUAL_PACKING_HEADCOUNT += Number(row.MANUAL_PACKING_HEADCOUNT);
            sumRow.OVERTIME_HEADCOUNT += Number(row.OVERTIME_HEADCOUNT);
            sumRow.AUTO_PACKING_QUANTITY += row.AUTO_PACKING_QUANTITY;
            sumRow.MANUAL_PACKING_QUANTITY += row.MANUAL_PACKING_QUANTITY;
            sumRow.TOTAL_FILLING_QUANTITY += row.TOTAL_FILLING_QUANTITY;
            sumRow.TOTAL_PACKING_QUANTITY += row.TOTAL_PACKING_QUANTITY;

            for (let key in row) {
                if (key.endsWith('_TARGET_WEIGHT') || key.endsWith('_PACKING_WEIGHT')) {
                    const lineName = key.split('_')[0];
                    if (!(key in sumRow)) {
                        sumRow[`${lineName}_TARGET_WEIGHT`] = 0;
                        sumRow[`${lineName}_PACKING_WEIGHT`] = 0;
                        sumRow[`${lineName}_ACHIEVEMENT_RATE`] = null;
                    }
                    sumRow[key] += row[key];

                    sumRow[`${lineName}_ACHIEVEMENT_RATE`] = sumRow[`${lineName}_TARGET_WEIGHT`] ? (sumRow[`${lineName}_PACKING_WEIGHT`] / sumRow[`${lineName}_TARGET_WEIGHT`]) : null;
                }
            }
            statReport.push(row);
        }
        if (statReport.length) {
            statReport.push({
                PACKING_DATE: prevPackingDate,
                PACKING_SHIFT: '小計',
                ...sumRow,
            });
        }
        obj.statReport = statReport;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingDailyReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢排程達成率統計表
export async function getPackingCompletionRateReport(user, packingDateStart, packingDateEnd) {
    const obj = {
        completionRateReport: [],
        error: null,
    };

    //額外條件
    if (!packingDateEnd) {
        packingDateEnd = packingDateStart;
    }

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const selectSQL = `
        SELECT S0.*, 
            S1.LINE_NAME AS PACKING_LINE_NAME,
            S1.PRINTER_IP AS DEFAULT_PRINTER_IP,
            S2.PALLET_NAME AS PACKING_PALLET_NAME,
            S3.PACKING_QUANTITY,
            (S3.PACKING_QUANTITY * S0.PACKING_WEIGHT_SPEC) AS PACKING_WEIGHT,
            S3.FIRST_CONFIRM_TIME, S3.LAST_CONFIRM_TIME, S3.MAX_DETAIL_SEQ_END, 
            ((S3.LAST_CONFIRM_TIME - S3.FIRST_CONFIRM_TIME) * 1440) AS PACKING_PERIOD
        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0
            LEFT JOIN AC.PBTC_IOT_PACKING_LINE S1
                ON S0.PACKING_LINE = S1.LINE_ID
                    AND S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
            LEFT JOIN AC.PBTC_IOT_PACKING_PALLET S2
                ON S0.PACKING_PALLET = S2.PALLET_ID
                    AND S0.COMPANY = S2.COMPANY
                    AND S0.FIRM = S2.FIRM
            LEFT JOIN (
                SELECT COMPANY, FIRM, 
                    PACKING_SEQ, 
                    SUM(GREATEST(DETAIL_SEQ_END - DETAIL_SEQ_START - SEQ_ERROR_COUNT + 1, 0)) AS PACKING_QUANTITY, 
                    MIN(CONFIRM_TIME) AS FIRST_CONFIRM_TIME, 
                    MAX(CONFIRM_TIME) AS LAST_CONFIRM_TIME, 
                    MAX(DETAIL_SEQ_END) AS MAX_DETAIL_SEQ_END
                FROM PBTC_IOT_PACKING_DETAIL
                WHERE 1 = 1
                GROUP BY COMPANY, FIRM, PACKING_SEQ ) S3
                ON S0.PACKING_SEQ = S3.PACKING_SEQ
                    AND S0.COMPANY = S3.COMPANY
                    AND S0.FIRM = S3.FIRM
        WHERE 1 = 1
            AND S0.COMPANY = :COMPANY 
            AND S0.FIRM = :FIRM 
            AND TRUNC(S0.PACKING_DATE) BETWEEN TO_DATE(:PACKING_DATE_START, 'YYYY-MM-DD') AND TO_DATE(:PACKING_DATE_END, 'YYYY-MM-DD') 
            AND S0.DELETE_TIME IS NULL 
        ORDER BY S0.PACKING_SEQ ASC
        `;

        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PACKING_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart },
            PACKING_DATE_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateEnd },
        };
        const options = { outFormat: oracledb.OBJECT };

        let result = await conn.execute(selectSQL, params, options);

        const completionRateReport = result.rows;
        //計算達成率、判斷是否達成
        completionRateReport.forEach(row => {
            row.ACHIEVEMENT_RATE = row.PACKING_WEIGHT / row.TARGET_WEIGHT;
            //包裝規格代碼開頭
            row.PACKING_MATERIAL_TYPE = ('' + row.PACKING_MATERIAL_ID)[0];
            //一個棧板的包裝重量，誤差超過這個數值就當作未達成
            //P開頭為紙袋、規格袋，每個棧板固定40包
            row.WEIGHT_PER_PALLET = ('P' === row.PACKING_MATERIAL_TYPE) ? +row.PACKING_WEIGHT_SPEC * 40 : +row.PACKING_WEIGHT_SPEC;
            row.WEIGHT_DIFF = Math.abs(row.TARGET_WEIGHT - row.PACKING_WEIGHT);
            // 有包裝: (排程-實際) < 一個棧板 → 達標
            // 未包裝: 未達標
            row.IS_COMPLETED = row.PACKING_WEIGHT ? (row.WEIGHT_DIFF < row.WEIGHT_PER_PALLET) : false;
        });

        obj.completionRateReport = completionRateReport;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingCompletionRateReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存排程達成率統計表
export async function savePackingCompletionRateReport(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const updateSQL = `
        UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
        SET 
            NOT_MEET_REASON     = :NOT_MEET_REASON,
            MEET_NOTE           = :MEET_NOTE,
            MEET_EDIT_USER_NAME = :MEET_EDIT_USER_NAME,
            MEET_EDIT_USER      = :MEET_EDIT_USER,
            MEET_EDIT_TIME      = SYSDATE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const params = {
                NOT_MEET_REASON: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOT_MEET_REASON },
                MEET_NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.MEET_NOTE },
                MEET_EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                MEET_EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
            };
            const result = await conn.execute(updateSQL, params, options);
            //console.log('update', result);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'savePackingCompletionRateReport 更新失敗');
            } else {
                obj.res.push(result);
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingCompletionRateReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢包裝費用統計表
export async function getPackingExpenseStatReport(user, packingDateStart, packingDateEnd) {
    const obj = {
        statReport: [],
        expenseTotalReport: [],
        packingStatReport: [],
        error: null,
    };

    //額外條件
    if (!packingDateEnd) {
        packingDateEnd = packingDateStart;
    }
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //每日包裝出勤表
        const attendanceReport = await getPackingDailyAttendanceReport(conn, user, packingDateStart, packingDateEnd);
        //每日包裝明細表
        const detailReport = await getPackingDailyDetailReport(conn, user, packingDateStart, packingDateEnd);
        //包裝項目單價表
        const expenseItems = await getPackingExpenseItems(conn, user);

        //單價表轉成Map
        const expenseItemMap = new Map();
        expenseItems.forEach(row => {
            expenseItemMap.set(row.ITEM_ID, {
                ITEM_NAME: row.ITEM_NAME,
                UNIT_PRICE: row.UNIT_PRICE,
                ITEM_ORDER: row.ITEM_ORDER,
            });
        });
        // console.log(expenseItemMap);

        //以出勤表為主檔，統計各費用項目
        const statMap = new Map();
        attendanceReport.forEach(row => {
            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            let stat;
            if (statMap.has(key)) {
                stat = statMap.get(key);
            } else {
                stat = {
                    PACKING_DATE: row.PACKING_DATE,
                    PACKING_SHIFT: row.PACKING_SHIFT,
                    OTHER_SEA_BULK_TANK: row.SEA_BULK_TANK, //SEA BULK槽車綁袋(次)
                    OTHER_FIBC_FILLING_TANK: row.FIBC_FILLING_TANK, //太空袋灌充槽車(包)
                    OTHER_PACKING_MATERIAL_CARRY: row.PACKING_MATERIAL_CARRY, //包材入廠搬運歸位(次)
                    OTHER_OFF_LINE_BAG_PRINT: row.OFF_LINE_BAG_PRINT, //OFF-LINE紙袋印製(噸)
                    OTHER_BAG_ATTACH_LABEL: row.BAG_ATTACH_LABEL, //紙袋專用標籤張貼(噸)
                    OTHER_BAG_STAMP: row.BAG_STAMP, //紙袋蓋專用章(噸)
                    OTHER_BAG_RESTACK: row.BAG_RESTACK, //紙袋重新自動堆疊(噸)
                    OTHER_BULK_WRAPPING: 0, //太空袋/八角箱捆膜(板)
                };
                statMap.set(key, stat);
            }
        });
        detailReport.forEach(row => {
            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            //主檔不存在就不處理
            if (!statMap.has(key)) {
                return;
            }
            const stat = statMap.get(key);
            const subtotal = {}; //要加總到主檔的欄位
            const packingMaterialType = row.PACKING_MATERIAL_ID[0]; //包材種類: P紙袋、T太空袋、C八角箱

            //換包數量
            if (row.SILO_NO.indexOf('換包') > -1) {
                let packingMaterialTypeFrom = 'P'; //預設為紙袋換包
                if ('太空袋換包' === row.SILO_NO) {
                    packingMaterialTypeFrom = 'T';
                } else if ('八角箱換包' === row.SILO_NO) {
                    packingMaterialTypeFrom = 'C';
                }
                if ('T' === packingMaterialType || 'C' === packingMaterialType) {
                    //太空袋、八角箱: 包數
                    subtotal[`REPACKING_${packingMaterialTypeFrom}_${packingMaterialType}`] = row.PACKING_DETAIL_COUNT + (row.REMAINDER_WEIGHT / row.PACKING_WEIGHT_SPEC);
                    subtotal.OTHER_BULK_WRAPPING = row.PACKING_DETAIL_COUNT; //太空袋/八角箱捆膜 (板)
                } else {
                    //紙袋: 重量(噸)
                    subtotal[`REPACKING_${packingMaterialTypeFrom}_${packingMaterialType}`] = (row.PACKING_TOTAL_WEIGHT + row.REMAINDER_WEIGHT) / 1000; //KG→MT
                }
            } else {
                //包裝數量
                if ('T' === packingMaterialType) { //太空袋
                    if (!row.PACKING_METHOD) {
                        row.PACKING_METHOD = '自動'; //包裝方式未選擇的預設值 (費用較低的)
                    }
                    if (row.PACKING_MATERIAL.indexOf('鋁箔') > -1) {
                        subtotal.PACKING_T_ALUMINUM = row.PACKING_DETAIL_COUNT + (row.REMAINDER_WEIGHT / row.PACKING_WEIGHT_SPEC);
                    } else if ('手動C線' === row.LINE_NAME && '自動' === row.PACKING_METHOD) {
                        subtotal.PACKING_T_AUTO = row.PACKING_DETAIL_COUNT + (row.REMAINDER_WEIGHT / row.PACKING_WEIGHT_SPEC);
                    } else {
                        subtotal.PACKING_T_MANUAL = row.PACKING_DETAIL_COUNT + (row.REMAINDER_WEIGHT / row.PACKING_WEIGHT_SPEC);
                    }
                    subtotal.OTHER_BULK_WRAPPING = row.PACKING_DETAIL_COUNT; //不含殘包
                } else if ('C' === packingMaterialType) { //八角箱
                    subtotal.PACKING_C_ALL = row.PACKING_DETAIL_COUNT + (row.REMAINDER_WEIGHT / row.PACKING_WEIGHT_SPEC);
                    subtotal.OTHER_BULK_WRAPPING = row.PACKING_DETAIL_COUNT; //太空袋/八角箱捆膜 (板)
                } else {
                    //P紙袋
                    subtotal[`PACKING_P_L${row.PACKING_LINE}`] = (row.PACKING_TOTAL_WEIGHT + row.REMAINDER_WEIGHT) / 1000; //KG→MT
                }
            }
            //清機
            if (row.CLEAN_TIMES) {
                subtotal[`CLEAN_${packingMaterialType}_L${row.PACKING_LINE}_1`] = row.CLEAN_TIMES;
                if ('T' === packingMaterialType || 'C' === packingMaterialType) { //清機(太空袋/八角箱)
                    subtotal[`CLEAN_T_C_L${row.PACKING_LINE}`] = row.CLEAN_TIMES;
                }
            }
            if (row.CLEAN_TIMES2) {
                subtotal[`CLEAN_${packingMaterialType}_L${row.PACKING_LINE}_2`] = row.CLEAN_TIMES2;
                if ('T' === packingMaterialType || 'C' === packingMaterialType) { //清機(太空袋/八角箱)
                    if (subtotal[`CLEAN_T_C_L${row.PACKING_LINE}`]) {
                        subtotal[`CLEAN_T_C_L${row.PACKING_LINE}`] += row.CLEAN_TIMES2;
                    } else {
                        subtotal[`CLEAN_T_C_L${row.PACKING_LINE}`] = row.CLEAN_TIMES2;
                    }
                }
            }

            //將細項加總到主檔
            Object.keys(subtotal).forEach(field => {
                if (stat[field]) {
                    stat[field] += subtotal[field];
                } else {
                    stat[field] = subtotal[field];
                }
            });
        });

        //需要合併的欄位
        const sumColMap = {
            'CLEAN_T_L3_1': 'CLEAN_T_C_L3', //清機-太空袋/八角箱-手動A線
            'CLEAN_T_L3_2': 'CLEAN_T_C_L3',
            'CLEAN_C_L3_1': 'CLEAN_T_C_L3',
            'CLEAN_C_L3_2': 'CLEAN_T_C_L3',
            'CLEAN_T_L4_1': 'CLEAN_T_C_L4', //清機-太空袋/八角箱-手動B線
            'CLEAN_T_L4_2': 'CLEAN_T_C_L4',
            'CLEAN_C_L4_1': 'CLEAN_T_C_L4',
            'CLEAN_C_L4_2': 'CLEAN_T_C_L4',
            'CLEAN_T_L5_1': 'CLEAN_T_C_L5', //清機-太空袋/八角箱-手動C線
            'CLEAN_T_L5_2': 'CLEAN_T_C_L5',
            'CLEAN_C_L5_1': 'CLEAN_T_C_L5',
            'CLEAN_C_L5_2': 'CLEAN_T_C_L5',
        };
        //加總合計
        const statReportColumns = [
            { LABEL_1_1: '自動', LABEL_1_2: 'A台', LABEL_1_3: '', LABEL_2_1: '紙袋', LABEL_2_2: '改紙袋', LABEL_3_1: '自動', LABEL_3_2: 'A台', LABEL_3_3: '1F', LABEL_4_1: '手動', LABEL_4_2: 'A台', values: ['PACKING_P_L1', 'REPACKING_P_P', 'CLEAN_P_L1_1', ['CLEAN_T_L3_1', 'CLEAN_T_L3_2', 'CLEAN_C_L3_1', 'CLEAN_C_L3_2'],] },
            { LABEL_1_1: '自動', LABEL_1_2: 'B台', LABEL_1_3: '', LABEL_2_1: '紙袋', LABEL_2_2: '改太空袋', LABEL_3_1: '自動', LABEL_3_2: 'A台', LABEL_3_3: '2F', LABEL_4_1: '手動', LABEL_4_2: 'B台', values: ['PACKING_P_L2', 'REPACKING_P_T', 'CLEAN_P_L1_2', ['CLEAN_T_L4_1', 'CLEAN_T_L4_2', 'CLEAN_C_L4_1', 'CLEAN_C_L4_2'],] },
            { LABEL_1_1: '手動', LABEL_1_2: 'A台', LABEL_1_3: '', LABEL_2_1: '紙袋', LABEL_2_2: '改八角箱', LABEL_3_1: '自動', LABEL_3_2: 'B台', LABEL_3_3: '1F', LABEL_4_1: '手動', LABEL_4_2: 'C台', values: ['PACKING_P_L3', 'REPACKING_P_C', 'CLEAN_P_L2_1', ['CLEAN_T_L5_1', 'CLEAN_T_L5_2', 'CLEAN_C_L5_1', 'CLEAN_C_L5_2'],] },
            { LABEL_1_1: '手動', LABEL_1_2: 'B台', LABEL_1_3: '', LABEL_2_1: '太空袋', LABEL_2_2: '改紙袋', LABEL_3_1: '自動', LABEL_3_2: 'B台', LABEL_3_3: '2F', LABEL_4_1: '＜～～其它事務～～＞', LABEL_4_2: '', values: ['PACKING_P_L4', 'REPACKING_T_P', 'CLEAN_P_L2_2', null,] },
            { LABEL_1_1: '手動', LABEL_1_2: 'C台', LABEL_1_3: '', LABEL_2_1: '太空袋', LABEL_2_2: '改八角箱', LABEL_3_1: '手動', LABEL_3_2: 'A台', LABEL_3_3: '1F', LABEL_4_1: '包材入廠搬運歸位 (次)', LABEL_4_2: '', values: ['PACKING_P_L5', 'REPACKING_T_C', 'CLEAN_P_L3_1', 'OTHER_PACKING_MATERIAL_CARRY',] },
            { LABEL_1_1: '手動', LABEL_1_2: 'D台', LABEL_1_3: '', LABEL_2_1: '八角箱', LABEL_2_2: '改太空袋', LABEL_3_1: '手動', LABEL_3_2: 'A台', LABEL_3_3: '2F', LABEL_4_1: 'OFF-LINE紙袋印製(噸)', LABEL_4_2: '', values: ['PACKING_P_L6', 'REPACKING_C_T', 'CLEAN_P_L3_2', 'OTHER_OFF_LINE_BAG_PRINT',] },
            { LABEL_1_1: '手動', LABEL_1_2: '太空袋', LABEL_1_3: '一般', LABEL_2_1: 'SEA BULK\n槽車綁袋', LABEL_2_2: '', LABEL_3_1: '手動', LABEL_3_2: 'B台', LABEL_3_3: '1F', LABEL_4_1: '紙袋專用標籤張貼 (噸)', LABEL_4_2: '', values: ['PACKING_T_MANUAL', 'OTHER_SEA_BULK_TANK', 'CLEAN_P_L4_1', 'OTHER_BAG_ATTACH_LABEL',] },
            { LABEL_1_1: '手動', LABEL_1_2: '太空袋', LABEL_1_3: '鋁箔', LABEL_2_1: '', LABEL_2_2: '', LABEL_3_1: '手動', LABEL_3_2: 'B台', LABEL_3_3: '2F', LABEL_4_1: '紙袋蓋專用章 (噸)', LABEL_4_2: '', values: ['PACKING_T_ALUMINUM', null, 'CLEAN_P_L4_2', 'OTHER_BAG_STAMP',] },
            { LABEL_1_1: '手動', LABEL_1_2: '太空袋', LABEL_1_3: '半自動', LABEL_2_1: '太空袋灌充槽車', LABEL_2_2: '', LABEL_3_1: '手動', LABEL_3_2: 'C台', LABEL_3_3: '1F', LABEL_4_1: '紙袋重新自動堆疊 (噸)', LABEL_4_2: '', values: ['PACKING_T_AUTO', 'OTHER_FIBC_FILLING_TANK', 'CLEAN_P_L5_1', 'OTHER_BAG_RESTACK',] },
            { LABEL_1_1: '手動', LABEL_1_2: '八角箱(含組裝)', LABEL_1_3: '', LABEL_2_1: '', LABEL_2_2: '', LABEL_3_1: '手動', LABEL_3_2: 'D台', LABEL_3_3: '1F', LABEL_4_1: '太空袋/八角箱捆膜 (板)', LABEL_4_2: '', values: ['PACKING_C_ALL', null, 'CLEAN_P_L6_1', 'OTHER_BULK_WRAPPING',] },
        ];
        //根據 日期+班別 統計各項目 數量、費用
        const shiftReport = [...statMap.values()];
        //不分日期的全部加總
        const allTotal = {
            PACKING_DATE: '*',
            PACKING_SHIFT: '*',
        };
        //同一日期、不同班別的加總
        const shiftTotal = {
            PACKING_DATE: null,
            '早': 0,
            '中': 0,
            '夜': 0,
            TOTAL: 0,
        };
        //分日期、不分班別的加總
        const dailyReportMap = new Map();
        //產生各種前端需要的表
        const statReport = []; //費用明細表
        //計算各項目的費用，並產生各個表格的資料
        shiftReport.forEach(row => {
            let shiftSum = 0; //班別小計
            let dailyTotal; //日期加總
            if (dailyReportMap.has(row.PACKING_DATE)) {
                dailyTotal = dailyReportMap.get(row.PACKING_DATE);
            } else {
                dailyTotal = {
                    PACKING_DATE: row.PACKING_DATE,
                };
                dailyReportMap.set(row.PACKING_DATE, dailyTotal);
            }
            //加總各項目
            Object.keys(row).forEach(field => {
                //跳過日期、班別欄位 (無法進行加總)
                if ('PACKING_DATE' === field || 'PACKING_SHIFT' === field) {
                    return;
                }
                //非數值、null、0 則不加總
                if (('number' !== typeof row[field]) || !row[field]) {
                    delete row[field];
                    return;
                }
                //單價未定義 則不加總
                if (!expenseItemMap.has(field)) {
                    delete row[field];
                    return;
                }
                //轉換包裝項目的格式
                row[field] = {
                    QTY: +row[field].toFixed(3),
                    ...expenseItemMap.get(field),
                };
                row[field].SUBTOTAL = row[field].QTY * row[field].UNIT_PRICE;
                row[field].SUBTOTAL = +row[field].SUBTOTAL.toFixed(3);
                shiftSum += row[field].SUBTOTAL;
                //統計全部日期的加總
                if (field in allTotal) {
                    allTotal[field].QTY += row[field].QTY;
                    allTotal[field].SUBTOTAL += row[field].SUBTOTAL;
                } else {
                    allTotal[field] = {
                        QTY: row[field].QTY,
                        SUBTOTAL: row[field].SUBTOTAL,
                        ...expenseItemMap.get(field),
                    };
                }
                //統計各日期的加總
                if (field in dailyTotal) {
                    dailyTotal[field].QTY += row[field].QTY;
                    dailyTotal[field].SUBTOTAL += row[field].SUBTOTAL;
                } else {
                    dailyTotal[field] = {
                        QTY: row[field].QTY,
                        SUBTOTAL: row[field].SUBTOTAL,
                        ...expenseItemMap.get(field),
                    };
                }
                //需要合併的欄位
                if (sumColMap[field]) {
                    const mergeField = sumColMap[field];
                    //統計全部日期的加總
                    if (mergeField in allTotal) {
                        allTotal[mergeField].QTY += row[field].QTY;
                        allTotal[mergeField].SUBTOTAL += row[field].SUBTOTAL;
                    } else {
                        allTotal[mergeField] = {
                            QTY: row[field].QTY,
                            SUBTOTAL: row[field].SUBTOTAL,
                            ...expenseItemMap.get(field),
                            ITEM_ORDER: 99999,
                        };
                    }
                    //統計各日期的加總
                    if (mergeField in dailyTotal) {
                        dailyTotal[mergeField].QTY += row[field].QTY;
                        dailyTotal[mergeField].SUBTOTAL += row[field].SUBTOTAL;
                    } else {
                        dailyTotal[mergeField] = {
                            QTY: row[field].QTY,
                            SUBTOTAL: row[field].SUBTOTAL,
                            ...expenseItemMap.get(field),
                            ITEM_ORDER: 99999,
                        };
                    }
                }
            });
            //日期+班別的小計
            row.TOTAL = {
                SUBTOTAL: shiftSum
            };

            if (shiftTotal.PACKING_DATE !== row.PACKING_DATE) {
                // console.log(shiftTotal);
                if (shiftTotal.PACKING_DATE) {
                    const report = {
                        PACKING_DATE: '當日總和',
                        PACKING_SHIFT: '',
                        LABEL_1_1: '早班包裝費用', LABEL_1_2: '', LABEL_1_3: '',
                        LABEL_2_1: '中班包裝費用', LABEL_2_2: '',
                        LABEL_3_1: '夜班包裝費用', LABEL_3_2: '', LABEL_3_3: '',
                        LABEL_4_1: '(早+中+夜) 包裝費用總計', LABEL_4_2: '',
                        QTY_0: null, EXPENSE_0: +shiftTotal['早'].toFixed(3),
                        QTY_1: null, EXPENSE_1: +shiftTotal['中'].toFixed(3),
                        QTY_2: null, EXPENSE_2: +shiftTotal['夜'].toFixed(3),
                        QTY_3: null, EXPENSE_3: +shiftTotal.TOTAL.toFixed(3),
                    };
                    statReport.push(report);
                }
                shiftTotal['早'] = 0;
                shiftTotal['中'] = 0;
                shiftTotal['夜'] = 0;
                shiftTotal.TOTAL = 0;
            }
            shiftTotal.PACKING_DATE = row.PACKING_DATE;
            shiftTotal[row.PACKING_SHIFT] = row.TOTAL.SUBTOTAL;
            shiftTotal.TOTAL += row.TOTAL.SUBTOTAL;

            statReportColumns.forEach(fieldRow => {
                const report = {
                    PACKING_DATE: row.PACKING_DATE,
                    PACKING_SHIFT: row.PACKING_SHIFT,
                    LABEL_1_1: fieldRow.LABEL_1_1,
                    LABEL_1_2: fieldRow.LABEL_1_2,
                    LABEL_1_3: fieldRow.LABEL_1_3,
                    LABEL_2_1: fieldRow.LABEL_2_1,
                    LABEL_2_2: fieldRow.LABEL_2_2,
                    LABEL_3_1: fieldRow.LABEL_3_1,
                    LABEL_3_2: fieldRow.LABEL_3_2,
                    LABEL_3_3: fieldRow.LABEL_3_3,
                    LABEL_4_1: fieldRow.LABEL_4_1,
                    LABEL_4_2: fieldRow.LABEL_4_2,
                };
                fieldRow.values.forEach((fields, fieldIndex) => {
                    let qty = 0;
                    let subtotal = 0;
                    if (Array.isArray(fields)) {
                        //針對 清機(太空袋/八角箱) 的特殊處理
                        fields.forEach(field => {
                            if (field in row) {
                                qty += row[field].QTY;
                                subtotal += row[field].SUBTOTAL;
                            }
                        });
                    } else {
                        if (fields in row) {
                            qty = row[fields].QTY;
                            subtotal = row[fields].SUBTOTAL;
                        }
                    }
                    report[`QTY_${fieldIndex}`] = qty ? qty : null;
                    report[`EXPENSE_${fieldIndex}`] = +subtotal.toFixed(3);
                });
                statReport.push(report);
            });
        });
        if (shiftTotal.PACKING_DATE) {
            const report = {
                PACKING_DATE: '當日總和',
                PACKING_SHIFT: '',
                LABEL_1_1: '早班包裝費用', LABEL_1_2: '', LABEL_1_3: '',
                LABEL_2_1: '中班包裝費用', LABEL_2_2: '',
                LABEL_3_1: '夜班包裝費用', LABEL_3_2: '', LABEL_3_3: '',
                LABEL_4_1: '(早+中+夜) 包裝費用總計', LABEL_4_2: '',
                QTY_0: null, EXPENSE_0: +shiftTotal['早'].toFixed(3),
                QTY_1: null, EXPENSE_1: +shiftTotal['中'].toFixed(3),
                QTY_2: null, EXPENSE_2: +shiftTotal['夜'].toFixed(3),
                QTY_3: null, EXPENSE_3: +shiftTotal.TOTAL.toFixed(3),
            };
            statReport.push(report);
        }
        // console.log(shiftReport);
        // console.log(statReport);
        // console.log(allTotal);
        // console.log(dailyReportMap);
        const expenseTotalReport = generateExpenseTotalReport(allTotal, expenseItemMap); //費用總表
        //產生每日數量表 (包裝量總計表、改包量統計表、清機統計表、其他事務統計表)
        const dailyReport = [];
        for (let [packingDate, row] of dailyReportMap.entries()) {
            const report = {
                PACKING_DATE: packingDate,
            };
            Object.keys(row).forEach(field => {
                //跳過日期、班別欄位 (無法進行加總)
                if ('PACKING_DATE' === field || 'PACKING_SHIFT' === field) {
                    return;
                }
                report[field] = +row[field].QTY.toFixed(3);
            });
            dailyReport.push(report);
        }
        //產生底部的加總欄位
        const dailyQtyRow = { PACKING_DATE: '合計重量/包/箱/次', };
        const dailyUnitPriceRow = { PACKING_DATE: '分類單價', };
        const dailySubtotalRow = { PACKING_DATE: '總計金額', };
        const dailyTotalRow = { PACKING_DATE: '合計', 'PACKING_P_L1': 0, 'REPACKING_P_P': 0, 'CLEAN_P_L1_1': 0, 'OTHER_PACKING_MATERIAL_CARRY': 0 };
        Object.keys(allTotal).forEach(field => {
            //跳過日期、班別欄位 (無法進行加總)
            if ('PACKING_DATE' === field || 'PACKING_SHIFT' === field) {
                return;
            }
            dailyQtyRow[field] = +allTotal[field].QTY.toFixed(3);
            dailyUnitPriceRow[field] = allTotal[field].UNIT_PRICE;
            dailySubtotalRow[field] = +allTotal[field].SUBTOTAL.toFixed(0);
            //處理各統計的合計金額
            if (allTotal[field].ITEM_ORDER < 1000) {
                dailyTotalRow.PACKING_P_L1 += +allTotal[field].SUBTOTAL.toFixed(0);
            } else if (allTotal[field].ITEM_ORDER < 2000) {
                dailyTotalRow.REPACKING_P_P += +allTotal[field].SUBTOTAL.toFixed(0);
            } else if (allTotal[field].ITEM_ORDER < 3000) {
                dailyTotalRow.CLEAN_P_L1_1 += +allTotal[field].SUBTOTAL.toFixed(0);
            } else if (allTotal[field].ITEM_ORDER < 4000) {
                dailyTotalRow.OTHER_PACKING_MATERIAL_CARRY += +allTotal[field].SUBTOTAL.toFixed(0);
            }
        });
        //處理沒有數量的包裝項目
        for (let [itemID, expenseItem] of expenseItemMap.entries()) {
            const fields = [itemID];
            if (sumColMap[itemID]) {
                const mergeField = sumColMap[itemID];
                fields.push(mergeField);
            }

            fields.forEach(field => {
                if (!dailyQtyRow[field]) {
                    dailyQtyRow[field] = 0;
                    dailyUnitPriceRow[field] = expenseItem.UNIT_PRICE;
                    dailySubtotalRow[field] = 0;
                }
            });
        }
        dailyTotalRow.PACKING_P_L1 = +dailyTotalRow.PACKING_P_L1.toFixed(0);
        dailyTotalRow.REPACKING_P_P = +dailyTotalRow.REPACKING_P_P.toFixed(0);
        dailyTotalRow.CLEAN_P_L1_1 = +dailyTotalRow.CLEAN_P_L1_1.toFixed(0);
        dailyTotalRow.OTHER_PACKING_MATERIAL_CARRY = +dailyTotalRow.OTHER_PACKING_MATERIAL_CARRY.toFixed(0);
        dailyReport.push(dailyQtyRow);
        dailyReport.push(dailyUnitPriceRow);
        dailyReport.push(dailySubtotalRow);
        dailyReport.push(dailyTotalRow);

        obj.statReport = statReport;
        obj.expenseTotalReport = expenseTotalReport;
        obj.packingStatReport = dailyReport;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingExpenseStatReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//產生包裝費用總表
function generateExpenseTotalReport(allTotal, expenseItemMap) {
    const expenseTotalReport = [
        { LABEL_1: '自動包裝', LABEL_2: '自動A(噸)', LABEL_3: '', QTY: 'PACKING_P_L1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '自動包裝', LABEL_2: '自動B(噸)', LABEL_3: '', QTY: 'PACKING_P_L2', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '手動A(噸)', LABEL_3: '', QTY: 'PACKING_P_L3', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '手動B(噸)', LABEL_3: '', QTY: 'PACKING_P_L4', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '手動C(噸)', LABEL_3: '', QTY: 'PACKING_P_L5', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '手動D(噸)', LABEL_3: '', QTY: 'PACKING_P_L6', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '太空包(包)', LABEL_3: '一般', QTY: 'PACKING_T_MANUAL', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '太空包(包)', LABEL_3: '鋁箔', QTY: 'PACKING_T_ALUMINUM', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '太空包(包)', LABEL_3: '半自動', QTY: 'PACKING_T_AUTO', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '手動包裝', LABEL_2: '八角箱(箱)', LABEL_3: '', QTY: 'PACKING_C_ALL', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋改包', LABEL_2: '改紙袋(噸)', LABEL_3: '', QTY: 'REPACKING_P_P', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋改包', LABEL_2: '改太空包(包)', LABEL_3: '', QTY: 'REPACKING_P_T', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋改包', LABEL_2: '改八角箱(箱)', LABEL_3: '', QTY: 'REPACKING_P_C', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '太空包改包', LABEL_2: '改紙袋(噸)', LABEL_3: '', QTY: 'REPACKING_T_P', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '太空包改包', LABEL_2: '改八角箱(箱)', LABEL_3: '', QTY: 'REPACKING_T_C', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '八角箱改包', LABEL_2: '改太空袋(箱)', LABEL_3: '', QTY: 'REPACKING_C_T', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: 'SEA BULK槽車綁袋(車)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_SEA_BULK_TANK', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '太空袋灌充槽車(包)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_FIBC_FILLING_TANK', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '自動A', LABEL_3: '1F', QTY: 'CLEAN_P_L1_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '自動A', LABEL_3: '2F', QTY: 'CLEAN_P_L1_2', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '自動B', LABEL_3: '1F', QTY: 'CLEAN_P_L2_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '自動B', LABEL_3: '2F', QTY: 'CLEAN_P_L2_2', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動A', LABEL_3: '1F', QTY: 'CLEAN_P_L3_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動A', LABEL_3: '2F', QTY: 'CLEAN_P_L3_2', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動B', LABEL_3: '1F', QTY: 'CLEAN_P_L4_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動B', LABEL_3: '2F', QTY: 'CLEAN_P_L4_2', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動C', LABEL_3: '1F', QTY: 'CLEAN_P_L5_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機(紙袋)', LABEL_2: '手動D', LABEL_3: '1F', QTY: 'CLEAN_P_L6_1', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機太空包/八角箱', LABEL_2: '手動A', LABEL_3: '1F', QTY: ['CLEAN_T_L3_1', 'CLEAN_T_L3_2', 'CLEAN_C_L3_1', 'CLEAN_C_L3_2'], UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機太空包/八角箱', LABEL_2: '手動B', LABEL_3: '1F', QTY: ['CLEAN_T_L4_1', 'CLEAN_T_L4_2', 'CLEAN_C_L4_1', 'CLEAN_C_L4_2'], UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '清機太空包/八角箱', LABEL_2: '手動C', LABEL_3: '1F', QTY: ['CLEAN_T_L5_1', 'CLEAN_T_L5_2', 'CLEAN_C_L5_1', 'CLEAN_C_L5_2'], UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '包材入廠搬運歸位(次)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_PACKING_MATERIAL_CARRY', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: 'OFF-LINE紙袋印製(噸)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_OFF_LINE_BAG_PRINT', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋專用標籤張貼(噸)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_BAG_ATTACH_LABEL', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋蓋專用章(噸)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_BAG_STAMP', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '紙袋重新自動堆疊(噸)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_BAG_RESTACK', UNIT_PRICE: 0, SUBTOTAL: 0 },
        { LABEL_1: '太空包/八角箱捆膜(板)', LABEL_2: '', LABEL_3: '', QTY: 'OTHER_BULK_WRAPPING', UNIT_PRICE: 0, SUBTOTAL: 0 },
    ];
    let sum = 0;
    expenseTotalReport.forEach(row => {
        let qty = 0;
        let unitPrice = 0;
        const fields = row.QTY;
        if (Array.isArray(fields)) {
            //針對 清機(太空袋/八角箱) 的特殊處理
            fields.forEach(field => {
                if (field in allTotal) {
                    qty += allTotal[field].QTY;
                    unitPrice = allTotal[field].UNIT_PRICE;
                } else if (expenseItemMap.has(field)) {
                    unitPrice = expenseItemMap.get(field).UNIT_PRICE;
                }
            });
        } else {
            if (fields in allTotal) {
                qty = allTotal[fields].QTY;
                unitPrice = allTotal[fields].UNIT_PRICE;
            } else if (expenseItemMap.has(fields)) {
                unitPrice = expenseItemMap.get(fields).UNIT_PRICE;
            }
        }
        row.QTY = +qty.toFixed(3);
        row.UNIT_PRICE = unitPrice;
        row.SUBTOTAL = +(qty * unitPrice).toFixed(0);
        sum += row.SUBTOTAL;
    });
    expenseTotalReport.push({ LABEL_1: '總計金額', LABEL_2: '', LABEL_3: '', QTY: null, UNIT_PRICE: null, SUBTOTAL: sum });
    expenseTotalReport.push({ LABEL_1: '手動包裝罰款金額', LABEL_2: '', LABEL_3: '', QTY: null, UNIT_PRICE: null, SUBTOTAL: 0 });
    expenseTotalReport.push({ LABEL_1: '合計', LABEL_2: '', LABEL_3: '', QTY: null, UNIT_PRICE: null, SUBTOTAL: sum });
    const tax = +(sum * 0.05).toFixed(0);
    expenseTotalReport.push({ LABEL_1: '5%稅金', LABEL_2: '', LABEL_3: '', QTY: null, UNIT_PRICE: null, SUBTOTAL: tax });
    expenseTotalReport.push({ LABEL_1: '請款金額(含稅)', LABEL_2: '', LABEL_3: '', QTY: null, UNIT_PRICE: null, SUBTOTAL: sum + tax });

    return expenseTotalReport;
}

//包裝費用統計表 - 包裝項目單價表
async function getPackingExpenseItems(conn, user) {
    const selectSQL = `
        SELECT *
        FROM AC.PBTC_IOT_PACKING_EXPENSE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_SHIFT = '*'
        ORDER BY ITEM_ORDER ASC
    `;

    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
    };
    const options = { outFormat: oracledb.OBJECT };

    let result = await conn.execute(selectSQL, params, options);
    // console.log(params, result);

    return result.rows;
}


//查詢包裝個人績效表
export async function getPackingPerformanceReport(user, packingDateStart, packingDateEnd) {
    const obj = {
        statReport: [],
        error: null,
    };

    //額外條件
    if (!packingDateEnd) {
        packingDateEnd = packingDateStart;
    }

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const attendanceReport = await getPackingDailyAttendanceReport(conn, user, packingDateStart, packingDateEnd);
        const detailReport = await getPackingDailyDetailReport(conn, user, packingDateStart, packingDateEnd);

        //排除查無資料的情況
        if (!attendanceReport || !attendanceReport.length) {
            return obj;
        }

        //以「每日包裝出勤及槽車灌充作業」為主，產生領班資料
        const reportMap = new Map();
        for (let i = 0; i < attendanceReport.length; i++) {
            const row = attendanceReport[i];
            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            reportMap.set(key, {
                ...row,
                FOREMAN1: 'NA' === row.FOREMAN1 ? null : row.FOREMAN1,
                FOREMAN2: 'NA' === row.FOREMAN2 ? null : row.FOREMAN2,
            });
        }

        //統計每天各班別的包裝量
        const detailMap = new Map();
        detailReport.forEach(row => {
            row.PACKING_DATE = moment(row.PACKING_DATE).format('YYYY-MM-DD');
            const key = `${row.PACKING_DATE}_${row.LINE_NAME}_${row.PACKING_SEQ}_${row.PACKING_SHIFT}`;
            let detail;
            if (detailMap.has(key)) {
                detail = detailMap.get(key);
            } else {
                detail = {
                    ...row,
                    PACKING_TOTAL_WEIGHT: 0,
                };
                detailMap.set(key, detail);
            }
            detail.PACKING_TOTAL_WEIGHT += row.PACKING_TOTAL_WEIGHT;
        });

        //整理報表資料
        const foremanMap = new Map();
        for (const row of detailMap.values()) {
            const key = `${row.PACKING_DATE}_${row.PACKING_SHIFT}`;
            if (!reportMap.has(key)) {
                console.warn('包裝統計表 找不到key', key, row);
                continue;
            }
            const report = reportMap.get(key);
            let foremanList = [];
            if (report.FOREMAN1) {
                foremanList.push(report.FOREMAN1);
            }
            if (report.FOREMAN2) {
                foremanList.push(report.FOREMAN2);
            }
            foremanList.forEach(foreman => {
                const key = foreman;
                let detail;
                if (foremanMap.has(key)) {
                    detail = foremanMap.get(key);
                } else {
                    detail = {
                        FOREMAN: foreman,
                        TOTAL_TARGET_WEIGHT: 0,
                        TOTAL_PACKING_WEIGHT: 0,
                        TOTAL_ACHIEVEMENT_RATE: 0,
                    };
                    foremanMap.set(key, detail);
                }

                detail.TOTAL_TARGET_WEIGHT += row.TARGET_WEIGHT / foremanList.length;
                detail.TOTAL_PACKING_WEIGHT += row.PACKING_TOTAL_WEIGHT / foremanList.length;
                detail.TOTAL_ACHIEVEMENT_RATE = detail.TOTAL_TARGET_WEIGHT ? (detail.TOTAL_PACKING_WEIGHT / detail.TOTAL_TARGET_WEIGHT) : null;
                detail[`${row.LINE_NAME}_TARGET_WEIGHT`] = ~~detail[`${row.LINE_NAME}_TARGET_WEIGHT`] + row.TARGET_WEIGHT;
                detail[`${row.LINE_NAME}_PACKING_WEIGHT`] = ~~detail[`${row.LINE_NAME}_PACKING_WEIGHT`] + row.PACKING_TOTAL_WEIGHT;
                detail[`${row.LINE_NAME}_ACHIEVEMENT_RATE`] = detail[`${row.LINE_NAME}_TARGET_WEIGHT`] ? (detail[`${row.LINE_NAME}_PACKING_WEIGHT`] / detail[`${row.LINE_NAME}_TARGET_WEIGHT`]) : null;
            });
        }
        // console.log(foremanMap);
        const statReport = [...foremanMap.values()];
        statReport.sort((a, b) => {
            return a.FOREMAN.localeCompare(b.FOREMAN);
        });

        obj.statReport = statReport;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingDailyReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢包裝項目單價表
export async function getPackingExpenseItemsReport(user) {
    const obj = {
        expenseItems: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //包裝項目單價表
        const expenseItems = await getPackingExpenseItems(conn, user);

        obj.expenseItems = expenseItems;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingExpenseItemsReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存包裝項目單價表
export async function savePackingExpenseItemsReport(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const updateSQL = `
        UPDATE AC.PBTC_IOT_PACKING_EXPENSE
        SET 
            UNIT_PRICE     = :UNIT_PRICE,
            NOTE           = :NOTE,
            EDIT_USER_NAME = :EDIT_USER_NAME,
            EDIT_USER      = :EDIT_USER,
            EDIT_TIME      = SYSDATE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_SHIFT = :PACKING_SHIFT
            AND ITEM_ID = :ITEM_ID
            AND (UNIT_PRICE <> :UNIT_PRICE
                OR NOTE <> :NOTE
                OR NOTE IS NULL)
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const params = {
                UNIT_PRICE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.UNIT_PRICE },
                NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.NOTE },
                EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                PACKING_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SHIFT },
                ITEM_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.ITEM_ID },
            };
            const result = await conn.execute(updateSQL, params, options);
            //console.log('update', result);
            obj.res.push(result);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'savePackingExpenseItems 更新失敗');
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingExpenseItems', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
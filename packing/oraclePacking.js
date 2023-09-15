import config from '../config.js';
import { getNowDatetimeString, firmToDept } from '../libs.js';
import * as PackingWork from './packingWork.js';
import * as PackingReport from './packingReport.js';
import * as Mailer from '../mailer.js';
import moment from 'moment';
import oracledb from 'oracledb';

//判斷包裝狀態是否屬於已結束，將不可進行 新增包裝項次、包裝結束等操作
export function isPackingStatusFinish(packingStatus) {
    if ('已完成' === packingStatus || '包裝取消' === packingStatus || '強制結束' === packingStatus) {
        return true;
    }

    return false;
}

//取得包裝作業基本資料
export async function getPackingOptions(user) {
    const obj = {
        lines: [],
        notes: [],
        materials: [],
        pallets: [],
        prints: [],
        prodLines: [],
        silos: [],
        weightSpecs: [],
        error: null,
    };

    let conn;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
    };
    const options = { outFormat: oracledb.OBJECT };
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const promises = [
            { field: 'detailReportReasons', sql: 'SELECT REASON_NAME FROM AC.PBTC_IOT_PACK_DETAIL_REASON WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY REASON_ORDER ASC', },
            { field: 'foremen', sql: 'SELECT FOREMAN_NAME FROM AC.PBTC_IOT_PACKING_FOREMAN WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY FOREMAN_ORDER ASC', },
            { field: 'lines', sql: 'SELECT LINE_ID, LINE_NAME FROM AC.PBTC_IOT_PACKING_LINE WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY LINE_ID ASC', },
            { field: 'materials', sql: 'SELECT MATERIAL_ID, MATERIAL_NAME FROM AC.PBTC_IOT_PACKING_MATERIAL WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY DISPLAY_ORDER ASC', },
            { field: 'notMeetReasons', sql: 'SELECT REASON_NAME FROM AC.PBTC_IOT_PACK_NOT_MEET_REASON WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY REASON_ORDER ASC', },
            { field: 'notes', sql: 'SELECT NOTE_ID, NOTE_NAME FROM AC.PBTC_IOT_PACKING_NOTE WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY NOTE_ID ASC', },
            { field: 'pallets', sql: 'SELECT PALLET_ID, PALLET_NAME FROM AC.PBTC_IOT_PACKING_PALLET WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY DISPLAY_ORDER ASC', },
            { field: 'printers', sql: 'SELECT PRINTER_NAME, PRINTER_IP, PRINTER_PORT FROM AC.PBTC_IOT_PRINTER_INFO WHERE COMPANY = :COMPANY AND FIRM = :FIRM AND TAG_KIND = \'ASRS_TAG\' ORDER BY PRINTER_ORDER ASC', },
            { field: 'prints', sql: 'SELECT PRD_PC, CUST_PRD_PC, GRADE, COLOR, BOTTOM FROM AC.PBTC_IOT_PACKING_PRINT WHERE COMPANY = :COMPANY AND FIRM = :FIRM', },
            { field: 'prodLines', sql: 'SELECT LINE FROM AC.PBTC_IOT_FEEDER_INFO WHERE COMPANY = :COMPANY AND FIRM = :FIRM GROUP BY LINE ORDER BY LINE ASC', },
            { field: 'shifts', sql: 'SELECT SHIFT_NAME FROM AC.PBTC_IOT_PACKING_SHIFT WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY SHIFT_ORDER ASC', },
            { field: 'silos', sql: 'SELECT SILO_NAME FROM AC.PBTC_IOT_PACKING_SILO WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY SILO_ORDER ASC', },
            { field: 'weightSpecs', sql: 'SELECT SPEC_ID FROM AC.PBTC_IOT_PACKING_WEIGHT_SPEC WHERE COMPANY = :COMPANY AND FIRM = :FIRM ORDER BY SPEC_ORDER ASC', },
        ].map(async table => {
            const result = await conn.execute(table.sql, params, options);

            if (result.rows && result.rows.length) {
                obj[table.field] = result.rows;
            }
        });
        await Promise.all(promises);
        
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingOptions', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢包裝排程
export async function getPackingSchedule(user, packingDateStart, packingDateEnd, queryMode, proLine, proSeq) {
    const obj = {
        schedules: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //額外條件
        if (!packingDateEnd) {
            packingDateEnd = packingDateStart;
        }
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PACKING_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart },
            PACKING_DATE_END: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateEnd },
        };
        let orderBy = 'S0.PACKING_DATE ASC, S0.SCHEDULE_ORDER ASC, S0.PACKING_SEQ ASC';

        let statusWhere = '';
        if ('undone' === queryMode) { //未完成包裝
            packingDateStart = '2023-04-01';
            params.PACKING_DATE_START = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart };
            statusWhere = 'AND (S0.PACKING_STATUS = \'包裝中\' OR S0.PACKING_STATUS IS NULL) ';
            orderBy = 'S0.PRD_PC ASC, S0.SCHEDULE_ORDER ASC, S0.PACKING_SEQ ASC';
        } else if ('proLine' === queryMode) { //線別+押出序號
            packingDateStart = '2022-12-01';
            packingDateEnd = moment().add(1, 'years').format('YYYY-MM-DD');
            params.PACKING_DATE_START = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart };
            params.PACKING_DATE_END = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateEnd };
            statusWhere = 'AND S0.PRO_SCHEDULE_LINE = :PRO_SCHEDULE_LINE AND S0.PRO_SCHEDULE_SEQ = :PRO_SCHEDULE_SEQ ';
            params.PRO_SCHEDULE_LINE = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + proLine };
            params.PRO_SCHEDULE_SEQ = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + proSeq };
        } else {
            params.PACKING_DATE_END = params.PACKING_DATE_START;
        }

        //修改SQL要記得連 getPackingScheduleBySeq, savePackingSchedule 那邊也改
        const sql = `
        SELECT S0.*, 
            S1.LINE_NAME AS PACKING_LINE_NAME,
            S1.PRINTER_IP AS DEFAULT_PRINTER_IP,
            S2.PALLET_NAME AS PACKING_PALLET_NAME,
            S3.PACKING_QUANTITY,
            (S3.PACKING_QUANTITY * S0.PACKING_WEIGHT_SPEC) AS TOTAL_PACKING_WEIGHT,
            LEAST(COALESCE(S1.PACKING_INTERVAL, S4.PACKING_INTERVAL), S4.PACKING_INTERVAL) AS MIN_PACKING_INTERVAL,
            (SELECT LISTAGG(LONO, ',') WITHIN GROUP(ORDER BY COMPANY, FIRM, PRD_PC) AS LONO 
            FROM AC.RM_STGFLD
            WHERE COMPANY = S0.COMPANY AND FIRM = S0.FIRM AND PRD_PC = S0.PRD_PC AND LOT_NO <> S0.LOT_NO AND STATUS = '1' AND IO = 'I'
            GROUP BY COMPANY, FIRM, PRD_PC) AS LONO
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
                    SUM(GREATEST(DETAIL_SEQ_END - DETAIL_SEQ_START - SEQ_ERROR_COUNT + 1, 0)) AS PACKING_QUANTITY
                FROM PBTC_IOT_PACKING_DETAIL
                WHERE 1 = 1
                GROUP BY COMPANY, FIRM, PACKING_SEQ ) S3
                ON S0.PACKING_SEQ = S3.PACKING_SEQ
                    AND S0.COMPANY = S3.COMPANY
                    AND S0.FIRM = S3.FIRM
            LEFT JOIN AC.PBTC_IOT_PACKING_MATERIAL S4
                ON S0.PACKING_MATERIAL = S4.MATERIAL_NAME
                    AND S0.COMPANY = S4.COMPANY
                    AND S0.FIRM = S4.FIRM
        WHERE 1 = 1
            AND S0.COMPANY = :COMPANY 
            AND S0.FIRM = :FIRM 
            AND TRUNC(S0.PACKING_DATE) BETWEEN TO_DATE(:PACKING_DATE_START, 'YYYY-MM-DD') AND TO_DATE(:PACKING_DATE_END, 'YYYY-MM-DD') 
            AND S0.DELETE_TIME IS NULL 
            ${statusWhere}
        ORDER BY ${orderBy} `;
        const options = { outFormat: oracledb.OBJECT };

        //包裝日期與今天的日期差，1為昨天、0今天、-1明天
        const packingDateDiffDays = moment().diff(params.PACKING_DATE_START.val, 'days');
        //假設星期一查詢上星期五，days為3，只有這段期間才觸發同步資料
        //考慮到連假，故日期範圍設定為5天
        if (packingDateDiffDays >= 0 && packingDateDiffDays <= 7) {
            await syncPackingSchedule(conn, user, packingDateStart);
        }

        // console.log(sql, params);
        const result = await conn.execute(sql, params, options);

        if (result.rows && result.rows.length) {
            obj.schedules = result.rows;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingSchedule', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢包裝排程(公司廠別序號)
export async function getPackingScheduleBySeq(conn, user, packingSeq) {
    const obj = {
        schedule: null,
        error: null,
    };

    //是否需要建立新連線
    const newConnection = !conn;
    try {
        if (newConnection) {
            conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        }

        const selectScheduleSQL = `
        SELECT S0.*, 
            S1.LINE_NAME AS PACKING_LINE_NAME,
            S1.PRINTER_IP AS DEFAULT_PRINTER_IP,
            S2.PALLET_NAME AS PACKING_PALLET_NAME,
            S3.PACKING_QUANTITY,
            (S3.PACKING_QUANTITY * S0.PACKING_WEIGHT_SPEC) AS TOTAL_PACKING_WEIGHT,
            LEAST(COALESCE(S1.PACKING_INTERVAL, S4.PACKING_INTERVAL), S4.PACKING_INTERVAL) AS MIN_PACKING_INTERVAL,
            (SELECT LISTAGG(LONO, ',') WITHIN GROUP(ORDER BY COMPANY, FIRM, PRD_PC) AS LONO 
            FROM AC.RM_STGFLD
            WHERE COMPANY = S0.COMPANY AND FIRM = S0.FIRM AND PRD_PC = S0.PRD_PC AND LOT_NO <> S0.LOT_NO AND STATUS = '1' AND IO = 'I'
            GROUP BY COMPANY, FIRM, PRD_PC) AS LONO
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
                    SUM(GREATEST(DETAIL_SEQ_END - DETAIL_SEQ_START - SEQ_ERROR_COUNT + 1, 0)) AS PACKING_QUANTITY
                FROM PBTC_IOT_PACKING_DETAIL
                WHERE 1 = 1
                GROUP BY COMPANY, FIRM, PACKING_SEQ ) S3
                ON S0.PACKING_SEQ = S3.PACKING_SEQ
                    AND S0.COMPANY = S3.COMPANY
                    AND S0.FIRM = S3.FIRM
            LEFT JOIN AC.PBTC_IOT_PACKING_MATERIAL S4
                ON S0.PACKING_MATERIAL = S4.MATERIAL_NAME
                    AND S0.COMPANY = S4.COMPANY
                    AND S0.FIRM = S4.FIRM
        WHERE 
            S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.PACKING_SEQ = :PACKING_SEQ
        `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingSeq },
        };
        const options = { outFormat: oracledb.OBJECT };

        // console.log(sql, params);
        const result = await conn.execute(selectScheduleSQL, params, options);
        if (result.rows && result.rows.length) {
            obj.schedule = result.rows[0];
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingScheduleBySeq', err);
        obj.error = err.toString();
    } finally {
        if (newConnection) {
            await conn.close();
        }
    }

    return obj;
}

//儲存包裝排程
export async function savePackingSchedule(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const insertSQL = `
        INSERT INTO AC.PBTC_IOT_PACKING_SCHEDULE (
            COMPANY, FIRM, DEPT, 
            WORK_SHIFT, PACKING_LINE, SILO_NO, 
            PRD_PC, LOT_NO, ORDER_WEIGHT, 
            SEQ_START, TARGET_WEIGHT, PACKING_NOTE, 
            IS_EMPTYING, 
            PACKING_MATERIAL, PACKING_PALLET, PACKING_GRADE, 
            PACKING_COLOR, PACKING_BOTTOM, PACKING_STATUS, 
            PACKING_WEIGHT_SPEC, PACKING_MATERIAL_ID, 
            DUPONT_PRINT, BARLOG_PRINT, 
            PRO_SCHEDULE_LINE, PRO_SCHEDULE_SEQ, 
            PRO_SCHEDULE_UKEY, CUST_PRD_PC, PACKING_SELECT, 
            PACKING_DATE, 
            CREATE_USER_NAME, CREATE_USER, CREATE_TIME, 
            PACKING_SEQ, SCHEDULE_ORDER 
        )
        VALUES (
            :COMPANY, :FIRM, :DEPT, 
            :WORK_SHIFT, :PACKING_LINE, :SILO_NO, 
            :PRD_PC, :LOT_NO, :ORDER_WEIGHT, 
            :SEQ_START, :TARGET_WEIGHT, :PACKING_NOTE, 
            :IS_EMPTYING, 
            :PACKING_MATERIAL, :PACKING_PALLET, :PACKING_GRADE, 
            :PACKING_COLOR, :PACKING_BOTTOM, :PACKING_STATUS, 
            :PACKING_WEIGHT_SPEC, :PACKING_MATERIAL_ID, 
            :DUPONT_PRINT, :BARLOG_PRINT, 
            :PRO_SCHEDULE_LINE, :PRO_SCHEDULE_SEQ, 
            :PRO_SCHEDULE_UKEY, :CUST_PRD_PC, :PACKING_SELECT, 
            :PACKING_DATE, 
            :CREATE_USER_NAME, :CREATE_USER, SYSDATE, 
            AC.GET_PBTC_PACKING_SCHEDULE_SEQ(:COMPANY, :FIRM, :PACKING_DATE), 
            MOD(AC.GET_PBTC_PACKING_SCHEDULE_SEQ(:COMPANY, :FIRM, :PACKING_DATE), 100000) * 10
        ) `;
        const updateSQL = `
        UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
        SET
            WORK_SHIFT = :WORK_SHIFT, 
            PACKING_LINE = :PACKING_LINE, 
            SILO_NO = :SILO_NO, 
            PRD_PC = :PRD_PC, 
            LOT_NO = :LOT_NO, 
            ORDER_WEIGHT = :ORDER_WEIGHT, 
            SEQ_START = :SEQ_START, 
            TARGET_WEIGHT = :TARGET_WEIGHT, 
            PACKING_NOTE = :PACKING_NOTE, 
            IS_EMPTYING = :IS_EMPTYING, 
            PACKING_MATERIAL = :PACKING_MATERIAL, 
            PACKING_PALLET = :PACKING_PALLET, 
            PACKING_GRADE = :PACKING_GRADE, 
            PACKING_COLOR = :PACKING_COLOR, 
            PACKING_BOTTOM = :PACKING_BOTTOM, 
            PACKING_WEIGHT_SPEC = :PACKING_WEIGHT_SPEC, 
            PACKING_MATERIAL_ID = :PACKING_MATERIAL_ID, 
            DUPONT_PRINT = :DUPONT_PRINT, 
            BARLOG_PRINT = :BARLOG_PRINT, 
            PRO_SCHEDULE_LINE = :PRO_SCHEDULE_LINE, 
            PRO_SCHEDULE_SEQ = :PRO_SCHEDULE_SEQ, 
            PRO_SCHEDULE_UKEY = :PRO_SCHEDULE_UKEY, 
            CUST_PRD_PC = :CUST_PRD_PC, 
            PACKING_SELECT = :PACKING_SELECT, 
            SCHEDULE_ORDER = :SCHEDULE_ORDER, 
            EDIT_USER_NAME = :EDIT_USER_NAME, 
            EDIT_USER = :EDIT_USER, 
            EDIT_TIME = SYSDATE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        //update的前置檢查
        const selectSqlBySeq = `
        SELECT S0.*
        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        //此處查詢要跟getPackingSchedule一致
        const selectSQL = `
        SELECT S0.*, 
            S1.LINE_NAME AS PACKING_LINE_NAME,
            S1.PRINTER_IP AS DEFAULT_PRINTER_IP,
            S2.PALLET_NAME AS PACKING_PALLET_NAME,
            S3.PACKING_QUANTITY,
            (S3.PACKING_QUANTITY * S0.PACKING_WEIGHT_SPEC) AS TOTAL_PACKING_WEIGHT,
            LEAST(COALESCE(S1.PACKING_INTERVAL, S4.PACKING_INTERVAL), S4.PACKING_INTERVAL) AS MIN_PACKING_INTERVAL,
            (SELECT LISTAGG(LONO, ',') WITHIN GROUP(ORDER BY COMPANY, FIRM, PRD_PC) AS LONO 
            FROM AC.RM_STGFLD
            WHERE COMPANY = S0.COMPANY AND FIRM = S0.FIRM AND PRD_PC = S0.PRD_PC AND LOT_NO <> S0.LOT_NO AND STATUS = '1' AND IO = 'I'
            GROUP BY COMPANY, FIRM, PRD_PC) AS LONO
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
                    SUM(GREATEST(DETAIL_SEQ_END - DETAIL_SEQ_START - SEQ_ERROR_COUNT + 1, 0)) AS PACKING_QUANTITY
                FROM PBTC_IOT_PACKING_DETAIL
                WHERE 1 = 1
                GROUP BY COMPANY, FIRM, PACKING_SEQ ) S3
                ON S0.PACKING_SEQ = S3.PACKING_SEQ
                    AND S0.COMPANY = S3.COMPANY
                    AND S0.FIRM = S3.FIRM
            LEFT JOIN AC.PBTC_IOT_PACKING_MATERIAL S4
                ON S0.PACKING_MATERIAL = S4.MATERIAL_NAME
                    AND S0.COMPANY = S4.COMPANY
                    AND S0.FIRM = S4.FIRM
        WHERE S0.ROWID = :NEW_ROWID `;
        //刪除包裝排程
        const deleteScheduleSQL = `
        UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
        SET
            DELETE_TIME = SYSDATE, 
            EDIT_USER_NAME = :EDIT_USER_NAME, 
            EDIT_USER = :EDIT_USER, 
            EDIT_TIME = SYSDATE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        let result = null;
        const rowIdList = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            //包裝排程日期
            const packingDate = new Date(row.PACKING_DATE);
            if (isNaN(packingDate)) {
                row.PACKING_DATE = new Date();
            } else {
                row.PACKING_DATE = packingDate;
            }
            //Number型態轉換
            row.ORDER_WEIGHT = Number(row.ORDER_WEIGHT);
            row.SEQ_START = Number(row.SEQ_START);
            row.TARGET_WEIGHT = Number(row.TARGET_WEIGHT);
            row.PRO_SCHEDULE_SEQ = Number(row.PRO_SCHEDULE_SEQ);
            row.SCHEDULE_ORDER = Number(row.SCHEDULE_ORDER);

            //根據PACKING_SEQ欄位判斷 新增/修改
            let isUpdate = Boolean(row.PACKING_SEQ);
            if (isUpdate) {
                result = await conn.execute(selectSqlBySeq, {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                    DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                    PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                }, options);
                if (!result.rows.length) {
                    throw new Error(`查無包裝排程 SEQ=${row.PACKING_SEQ}`);
                }
                const scheduleFromDB = result.rows[0];
                if (isPackingStatusFinish(scheduleFromDB.PACKING_STATUS)) {
                    throw new Error('包裝狀態已結束或取消，不可編輯此排程');
                }

                //判斷是否修改 包裝日期
                if (moment(row.PACKING_DATE).format('YYYY-MM-DD') !== moment(scheduleFromDB.PACKING_DATE).format('YYYY-MM-DD')) {
                    if (scheduleFromDB.PACKING_STATUS || scheduleFromDB.PACKING_QUANTITY) {
                        throw new Error('此排程已進行包裝，無法修改日期');
                    }

                    //更新排程狀態
                    const params = {
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleFromDB.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleFromDB.FIRM },
                        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleFromDB.DEPT },
                        PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleFromDB.PACKING_SEQ },
                        EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                        EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                    };
                    result = await conn.execute(deleteScheduleSQL, params, options);
                    isUpdate = false; //改成Insert模式
                }
            }

            console.log(getNowDatetimeString(), `savePackingSchedule Date=${moment(row.PACKING_DATE).format('YYYY-MM-DD')}, SHIFT=${row.WORK_SHIFT}, SILO_NO=${row.SILO_NO}, PRD_PC=${row.PRD_PC}, LOT_NO=${row.LOT_NO}, SEQ=${row.PACKING_SEQ}`);

            if (isUpdate) {
                //UPDATE
                const params = {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                    DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                    WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.WORK_SHIFT },
                    PACKING_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_LINE },
                    SILO_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SILO_NO },
                    PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC },
                    LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOT_NO },
                    ORDER_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.ORDER_WEIGHT },
                    SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.SEQ_START },
                    TARGET_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.TARGET_WEIGHT },
                    PACKING_NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_NOTE },
                    IS_EMPTYING: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.IS_EMPTYING ? 1 : null },
                    PACKING_MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_MATERIAL },
                    PACKING_PALLET: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_PALLET },
                    PACKING_GRADE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_GRADE },
                    PACKING_COLOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_COLOR },
                    PACKING_BOTTOM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_BOTTOM },
                    PACKING_WEIGHT_SPEC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_WEIGHT_SPEC },
                    PACKING_MATERIAL_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_MATERIAL_ID },
                    DUPONT_PRINT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DUPONT_PRINT },
                    BARLOG_PRINT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.BARLOG_PRINT },
                    PRO_SCHEDULE_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRO_SCHEDULE_LINE },
                    PRO_SCHEDULE_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.PRO_SCHEDULE_SEQ },
                    PRO_SCHEDULE_UKEY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRO_SCHEDULE_UKEY },
                    CUST_PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.CUST_PRD_PC },
                    PACKING_SELECT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SELECT },
                    SCHEDULE_ORDER: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.SCHEDULE_ORDER },
                    /*包裝日期不可修改*/ //PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PACKING_DATE }, 
                    EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                    EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                    PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                };
                result = await conn.execute(updateSQL, params, options);
                if (!result.rowsAffected) {
                    console.error(getNowDatetimeString(), 'savePackingSchedule 更新失敗');
                }
                // console.log('update', result);
            } else {
                //INSERT
                const params = {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.DEPT },
                    WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.WORK_SHIFT },
                    PACKING_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_LINE },
                    SILO_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SILO_NO },
                    PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC },
                    LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOT_NO },
                    ORDER_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.ORDER_WEIGHT },
                    SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.SEQ_START },
                    TARGET_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.TARGET_WEIGHT },
                    PACKING_NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_NOTE },
                    IS_EMPTYING: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.IS_EMPTYING ? 1 : null },
                    PACKING_MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_MATERIAL },
                    PACKING_PALLET: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_PALLET },
                    PACKING_GRADE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_GRADE },
                    PACKING_COLOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_COLOR },
                    PACKING_BOTTOM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_BOTTOM },
                    PACKING_STATUS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_STATUS },
                    PACKING_WEIGHT_SPEC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_WEIGHT_SPEC },
                    PACKING_MATERIAL_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_MATERIAL_ID },
                    DUPONT_PRINT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DUPONT_PRINT },
                    BARLOG_PRINT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.BARLOG_PRINT },
                    PRO_SCHEDULE_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRO_SCHEDULE_LINE },
                    PRO_SCHEDULE_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.PRO_SCHEDULE_SEQ },
                    PRO_SCHEDULE_UKEY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRO_SCHEDULE_UKEY },
                    CUST_PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.CUST_PRD_PC },
                    PACKING_SELECT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SELECT },
                    PACKING_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PACKING_DATE },
                    CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                    CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                };
                result = await conn.execute(insertSQL, params, options);
                if (!result.rowsAffected) {
                    console.error(getNowDatetimeString(), 'savePackingSchedule 新增失敗');
                }
                // console.log('insert', result);
            }
            if (result && result.lastRowid) {
                rowIdList.push(result.lastRowid);
                await syncPackingSchedule(conn, user, row.PACKING_DATE);
            }
        }
        if (rows.length) {
            await conn.commit();

            for (let i = 0; i < rowIdList.length; i++) {
                const rowId = rowIdList[i];
                result = await conn.execute(selectSQL, {
                    NEW_ROWID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: rowId },
                }, options);
                if (result.rows && result.rows.length) {
                    const newScheduleData = result.rows[0];
                    obj.res.push(newScheduleData);
                }
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingSchedule', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新包裝排程
export async function updatePackingSchedule(conn, user, packingSeq, data) {
    const obj = {
        res: false,
        error: null,
    };

    //是否需要建立新連線
    const newConnection = !conn;
    try {
        if (newConnection) {
            conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        }

        const fields = Object.keys(data);
        if (fields.length <= 0) {
            throw new Error('data為空');
        }

        //更新包裝排程
        const updateSQL = `
            UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
            SET
                ${fields.map(field => field + ' = :' + field + ', ').join('\n')}
                EDIT_USER_NAME = :EDIT_USER_NAME, 
                EDIT_USER = :EDIT_USER, 
                EDIT_TIME = SYSDATE
            WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND PACKING_SEQ = :PACKING_SEQ 
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: true };

        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.DEPT },
            PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingSeq },
            EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
            EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
        };
        fields.forEach(field => {
            params[field] = data[field];
        });
        //console.log(updateSQL, params);
        await conn.execute(updateSQL, params, options);
    } catch (err) {
        console.error(getNowDatetimeString(), `updatePackingSchedule[PACKING_SEQ=${packingSeq}]`, err);
        obj.error = err.toString();
    } finally {
        if (newConnection) {
            await conn.close();
        }
    }

    return obj;
}

//刪除包裝排程
export async function deletePackingSchedule(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const deleteScheduleSQL = `
        UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
        SET
            DELETE_TIME = SYSDATE, 
            EDIT_USER_NAME = :EDIT_USER_NAME, 
            EDIT_USER = :EDIT_USER, 
            EDIT_TIME = SYSDATE
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        const selectScheduleSQL = `
        SELECT S0.*
        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0
        WHERE 
            S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.PACKING_SEQ = :PACKING_SEQ
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            //檢查排程目前狀態
            let result = await conn.execute(selectScheduleSQL, {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
            }, options);
            if (result.rows && result.rows.length) {
                const scheduleRow = result.rows[0];
                if (scheduleRow.DELETE_TIME) {
                    throw new Error('包裝排程已經刪除');
                }
                if (scheduleRow.PACKING_STATUS) {
                    throw new Error('包裝排程已經開始或結束，無法刪除此排程');
                }
            } else {
                //包裝排程不存在
                if (i > 0) {
                    await conn.rollback();
                }
                throw new Error('包裝排程不存在，無法刪除此排程');
            }

            //更新排程狀態
            const params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
            };
            result = await conn.execute(deleteScheduleSQL, params, options);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'deletePackingSchedule 刪除失敗');
            }
        }
        if (rows.length) {
            await conn.commit();
            obj.res = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'deletePackingSchedule', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//結束包裝排程
export async function finishPackingSchedule(user, scheduleData, date, invShtNo, packingStatus) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const scheduleSQL = `
            UPDATE AC.PBTC_IOT_PACKING_SCHEDULE
            SET
                PACKING_STATUS = :PACKING_STATUS, 
                INV_SHTNO = :INV_SHTNO,
                PACKING_FINISH_TIME = SYSDATE, 
                EDIT_USER_NAME = :EDIT_USER_NAME, 
                EDIT_USER = :EDIT_USER, 
                EDIT_TIME = SYSDATE
            WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PACKING_SEQ = :PACKING_SEQ
                AND PACKING_STATUS = '包裝中'
        `;
        const detailSQL = `
            UPDATE AC.PBTC_IOT_PACKING_DETAIL
            SET
                INV_TIME = :INV_TIME, 
                EDIT_USER_NAME = :EDIT_USER_NAME, 
                EDIT_USER = :EDIT_USER, 
                EDIT_TIME = SYSDATE
            WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PACKING_SEQ = :PACKING_SEQ
                AND INV_TIME IS NULL
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.FIRM },
            PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.PACKING_SEQ },
            EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
            EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
        };
        //更新包裝排程的狀態
        if (!isPackingStatusFinish(packingStatus)) {
            packingStatus = '已完成'; //預防輸入其他的狀態文字
        }
        scheduleData.PACKING_STATUS = packingStatus;
        await conn.execute(scheduleSQL, {
            ...params,
            PACKING_STATUS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingStatus },
            INV_SHTNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invShtNo },
        }, options);
        //更新包裝項次的過帳時間
        await conn.execute(detailSQL, {
            ...params,
            INV_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
        }, options);

        await conn.commit();
        obj.res = true;

        //檢查是否仍有殘包(有相同品番、不同批號)並寄信通知
        const remainderSql = `
            SELECT *
            FROM AC.RM_STGFLD
            WHERE 1 = 1
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND STATUS = '1'
                AND IO = 'I'
                AND PRD_PC = :PRD_PC
                AND LOT_NO <> :LOT_NO
        `;
        let remainderResult = await conn.execute(remainderSql, {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.FIRM },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.PRD_PC },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scheduleData.LOT_NO },
        }, options);
        if (remainderResult.rows && remainderResult.rows.length) {
            //觸發寄信、不等待結果
            Mailer.alarmOnPackingScheduleFinish(user, scheduleData, remainderResult.rows).catch(err => {
                console.error(getNowDatetimeString(), 'alarmOnPackingScheduleFinish', err);
            });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'finishPackingSchedule', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//同步生產排程資料到包裝排程
export async function syncPackingSchedule(conn, user, packingDateStart) {
    if (packingDateStart instanceof Date) {
        packingDateStart = moment(packingDateStart).format('YYYY-MM-DD');
    }

    const sql = `
    UPDATE PBTC_IOT_PACKING_SCHEDULE
    SET
        PRD_PC = ( SELECT PRD_PC FROM PRO_SCHEDULE WHERE UKEY = PRO_SCHEDULE_UKEY ),
        LOT_NO = ( SELECT LOT_NO FROM PRO_SCHEDULE WHERE UKEY = PRO_SCHEDULE_UKEY ),
        ORDER_WEIGHT = ( SELECT PRO_WT FROM PRO_SCHEDULE WHERE UKEY = PRO_SCHEDULE_UKEY )
    WHERE 1 = 1
        AND SILO_NO NOT LIKE '%換包'
        AND DELETE_TIME IS NULL
        AND PACKING_MATERIAL NOT LIKE '杜邦%'
        AND PACKING_MATERIAL <> 'BARLOG'
        AND PACKING_STATUS IS NULL
        AND PRO_SCHEDULE_UKEY IS NOT NULL
        AND COMPANY = :COMPANY
        AND FIRM = :FIRM
        AND TRUNC(PACKING_DATE) = TO_DATE(:PACKING_DATE_START, 'YYYY-MM-DD') 
`;
    const options = { outFormat: oracledb.OBJECT, autoCommit: true };
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        PACKING_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingDateStart },
    };
    return conn.execute(sql, params, options);
}

//查詢包裝項次
export async function getPackingDetail(user, packingSeq) {
    const obj = {
        schedule: null,
        details: [],
        prevDetail: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //修改SQL要記得連 savePackingSchedule 那邊也改
        const selectSQL = `
        SELECT S0.*,
            S1.PRD_PC, S1.CUST_PRD_PC, S1.LOT_NO, S1.PACKING_STATUS
        FROM AC.PBTC_IOT_PACKING_DETAIL S0
            LEFT JOIN AC.PBTC_IOT_PACKING_SCHEDULE S1 ON S0.COMPANY = S1.COMPANY AND S0.FIRM = S1.FIRM AND S0.PACKING_SEQ = S1.PACKING_SEQ
        WHERE 1 = 1
            AND S0.COMPANY = :COMPANY 
            AND S0.FIRM = :FIRM 
            AND S0.PACKING_SEQ = :PACKING_SEQ
        ORDER BY S0.DETAIL_ID ASC `;
        //查詢同一個公令+包裝機的最後一個包裝項次
        const lastDetailSQL = `
        SELECT * FROM (
            SELECT S2.*
            FROM AC.PBTC_IOT_PACKING_SCHEDULE S0 
                JOIN (SELECT COMPANY, FIRM, PRO_SCHEDULE_LINE, PRO_SCHEDULE_SEQ, PACKING_LINE, PACKING_MATERIAL_ID
                    FROM AC.PBTC_IOT_PACKING_SCHEDULE S1
                    WHERE COMPANY = :COMPANY 
                        AND FIRM = :FIRM 
                        AND PACKING_SEQ = :PACKING_SEQ) S1
                    ON S0.COMPANY = S1.COMPANY
                        AND S0.FIRM = S1.FIRM
                        AND S0.PRO_SCHEDULE_LINE = S1.PRO_SCHEDULE_LINE
                        AND S0.PRO_SCHEDULE_SEQ = S1.PRO_SCHEDULE_SEQ
                        AND S0.PACKING_MATERIAL_ID = S1.PACKING_MATERIAL_ID
                JOIN AC.PBTC_IOT_PACKING_DETAIL S2
                    ON S0.COMPANY = S2.COMPANY
                        AND S0.FIRM = S2.FIRM
                        AND S0.PACKING_SEQ = S2.PACKING_SEQ
            WHERE 1 = 1
                AND DELETE_TIME IS NULL
            ORDER BY S2.CONFIRM_TIME DESC
        )
        WHERE ROWNUM < 2 `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + packingSeq },
        };
        const options = { outFormat: oracledb.OBJECT };

        //查詢包裝排程
        const packingScheduleResult = await getPackingScheduleBySeq(null, user, packingSeq);
        if (packingScheduleResult.error) {
            throw new Error(packingScheduleResult.error);
        }
        if (!packingScheduleResult.schedule) {
            throw new Error('查無包裝排程');
        }
        obj.schedule = packingScheduleResult.schedule;

        //查詢包裝項次
        let result = await conn.execute(selectSQL, params, options);

        if (result.rows && result.rows.length) {
            obj.details = result.rows;
        }

        result = await conn.execute(lastDetailSQL, params, options);
        if (result.rows && result.rows.length) {
            obj.prevDetail = result.rows;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackingDetail', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//儲存包裝項次
export async function savePackingDetail(user, rows) {
    const obj = {
        res: false,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const mergeSQL = `
        MERGE INTO AC.PBTC_IOT_PACKING_DETAIL USING DUAL ON (
            COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND PACKING_SEQ = :PACKING_SEQ
                AND DETAIL_ID = :DETAIL_ID
        )
        WHEN MATCHED THEN 
            UPDATE SET 
                PALLET_NO = :PALLET_NO,
                DETAIL_SEQ_START = :DETAIL_SEQ_START, 
                DETAIL_SEQ_END = :DETAIL_SEQ_END, 
                SEQ_ERROR_COUNT = :SEQ_ERROR_COUNT, 
                PRINT_LABEL_TIME = :PRINT_LABEL_TIME, 
                PHOTO_URL = :PHOTO_URL, 
                CONFIRM_TIME = :CONFIRM_TIME, 
                INV_TIME = :INV_TIME, 
                DETAIL_NOTE = :DETAIL_NOTE, 
                LABEL_SEQ_START = :LABEL_SEQ_START, 
                LABEL_SEQ_END = :LABEL_SEQ_END, 
                IS_CONTINUE = :IS_CONTINUE, 
                EDIT_USER_NAME = :EDIT_USER_NAME, 
                EDIT_USER = :EDIT_USER, 
                EDIT_TIME = SYSDATE
        WHEN NOT MATCHED THEN 
            INSERT (
                COMPANY, FIRM, DEPT, 
                PACKING_SEQ, DETAIL_ID, PALLET_NO,
                DETAIL_SEQ_START, DETAIL_SEQ_END, SEQ_ERROR_COUNT, 
                PRINT_LABEL_TIME, PHOTO_URL, CONFIRM_TIME, INV_TIME, 
                DETAIL_NOTE, LABEL_SEQ_START, LABEL_SEQ_END, IS_CONTINUE, 
                CREATE_USER_NAME, CREATE_USER, CREATE_TIME
            )
            VALUES (
                :COMPANY, :FIRM, :DEPT, 
                :PACKING_SEQ, :DETAIL_ID, :PALLET_NO,
                :DETAIL_SEQ_START, :DETAIL_SEQ_END, :SEQ_ERROR_COUNT, 
                :PRINT_LABEL_TIME, :PHOTO_URL, :CONFIRM_TIME, :INV_TIME, 
                :DETAIL_NOTE, :LABEL_SEQ_START, :LABEL_SEQ_END, :IS_CONTINUE, 
                :CREATE_USER_NAME, :CREATE_USER, SYSDATE
            )
        `;
        const updateSQL = `
        UPDATE AC.PBTC_IOT_PACKING_DETAIL S0
        SET
            PALLET_WEIGHT = (GREATEST((S0.DETAIL_SEQ_END - S0.DETAIL_SEQ_START - S0.SEQ_ERROR_COUNT + 1), 0) * (
                SELECT PACKING_WEIGHT_SPEC
                FROM AC.PBTC_IOT_PACKING_SCHEDULE S1
                WHERE S0.COMPANY = S1.COMPANY AND S0.FIRM = S1.FIRM AND S0.PACKING_SEQ = S1.PACKING_SEQ))
        WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PACKING_SEQ = :PACKING_SEQ
        `;
        const selectScheduleSQL = `
        SELECT S0.*, S1.DETAIL_ID
        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0
            LEFT JOIN AC.PBTC_IOT_PACKING_DETAIL S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.PACKING_SEQ = S1.PACKING_SEQ
                AND S1.DETAIL_ID = :DETAIL_ID
        WHERE 
            S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.PACKING_SEQ = :PACKING_SEQ
        `;
        //此處查詢要跟getPackingDetail一致
        const selectSQL = `
            SELECT S0.*,
                S1.PRD_PC, S1.CUST_PRD_PC, S1.LOT_NO, S1.PACKING_STATUS
            FROM AC.PBTC_IOT_PACKING_DETAIL S0
                LEFT JOIN AC.PBTC_IOT_PACKING_SCHEDULE S1 ON S0.COMPANY = S1.COMPANY AND S0.FIRM = S1.FIRM AND S0.PACKING_SEQ = S1.PACKING_SEQ
            WHERE 
                S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                AND S0.PACKING_SEQ = :PACKING_SEQ
                AND S0.DETAIL_ID = :DETAIL_ID
            `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        obj.res = [];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            //自動產生確認時間
            if (!row.CONFIRM_TIME) {
                row.CONFIRM_TIME = new Date();
            }
            //Number型態轉換
            row.DETAIL_ID = Number(row.DETAIL_ID);
            row.DETAIL_SEQ_START = Number(row.DETAIL_SEQ_START);
            row.DETAIL_SEQ_END = Number(row.DETAIL_SEQ_END);
            row.SEQ_ERROR_COUNT = Number(row.SEQ_ERROR_COUNT);
            row.LABEL_SEQ_START = Number(row.LABEL_SEQ_START ?? undefined);
            row.LABEL_SEQ_END = Number(row.LABEL_SEQ_END ?? undefined);
            //NaN改成null
            ['LABEL_SEQ_START', 'LABEL_SEQ_END'].forEach(field => {
                if (isNaN(row[field])) {
                    row[field] = null;
                }
            });
            //Date型態轉換
            ['PRINT_LABEL_TIME', 'CONFIRM_TIME', 'INV_TIME'].forEach(field => {
                if ('string' === typeof row[field]) {
                    row[field] = new Date(row[field]);
                }
            });
            // console.log(row);

            //檢查包裝項次是否可以儲存
            let result = await conn.execute(selectScheduleSQL, {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                DETAIL_ID: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_ID },
            }, options);
            if (result.rows && result.rows.length) {
                const scheduleRow = result.rows[0];
                const isInsert = (null === scheduleRow.DETAIL_ID); //包裝項次是否為INSERT
                const isScheduleFinish = isPackingStatusFinish(scheduleRow.PACKING_STATUS);
                if (isScheduleFinish && isInsert) {
                    throw new Error('包裝排程已經結束，無法儲存包裝項次');
                }
                if (scheduleRow.DELETE_TIME) {
                    throw new Error('包裝排程已經刪除，無法儲存包裝項次');
                }

                const seqResult = await checkPackingDetailSeq(conn, user, scheduleRow, row);
                //console.log(seqResult);
                if (('續包棧板' !== row.DETAIL_NOTE) && (seqResult.count > 0)) {
                    //續包棧板 必定會發生序號重複，故特別排除
                    throw new Error(`包裝序號${row.DETAIL_SEQ_START}~${row.DETAIL_SEQ_END}與其他排程重複，請重新輸入`);
                }
            } else {
                //包裝排程不存在
                if (i > 0) {
                    await conn.rollback();
                }
                throw new Error('包裝排程不存在，無法儲存包裝項次');
            }

            const params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: firmToDept.get(user.FIRM) || user.DEPT },
                PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                DETAIL_ID: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_ID },
                PALLET_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PALLET_NO },
                DETAIL_SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_SEQ_START },
                DETAIL_SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_SEQ_END },
                SEQ_ERROR_COUNT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.SEQ_ERROR_COUNT },
                PRINT_LABEL_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.PRINT_LABEL_TIME },
                PHOTO_URL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PHOTO_URL },
                CONFIRM_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.CONFIRM_TIME },
                INV_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: row.INV_TIME },
                DETAIL_NOTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DETAIL_NOTE },
                LABEL_SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.LABEL_SEQ_START },
                LABEL_SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.LABEL_SEQ_END },
                IS_CONTINUE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.IS_CONTINUE ? 1 : null },
                CREATE_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                CREATE_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
                EDIT_USER_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.NAME },
                EDIT_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE },
            };
            result = await conn.execute(mergeSQL, params, options);
            //console.log('merge', result);
            if (!result.rowsAffected) {
                console.error(getNowDatetimeString(), 'savePackingDetail 更新失敗');
            } else {
                //計算棧板重量
                await conn.execute(updateSQL, {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
                    PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                }, options);
            }
            if (result && result.lastRowid) {
                result = await conn.execute(selectSQL, {
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
                    PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PACKING_SEQ },
                    DETAIL_ID: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_ID },
                }, options);
                if (result.rows && result.rows.length) {
                    obj.res.push(result.rows[0]);
                }

                //連動包裝排程的狀態
                if (!row.PACKING_STATUS) {
                    await updatePackingSchedule(conn, user, row.PACKING_SEQ, {
                        PACKING_STATUS: '包裝中',
                    });
                    row.PACKING_STATUS = '包裝中';
                }
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'savePackingDetail', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//檢查包裝序號是否重複
export async function checkPackingDetailSeq(conn, user, schedule, row) {
    const obj = {
        count: 0,
        error: null,
    };

    //是否需要建立新連線
    const newConnection = !conn;
    try {
        if (newConnection) {
            conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        }

        const selectSQL = `
        SELECT COUNT(1) AS COUNT
        FROM AC.PBTC_IOT_PACKING_SCHEDULE S0
            JOIN AC.PBTC_IOT_PACKING_DETAIL S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.PACKING_SEQ = S1.PACKING_SEQ
        WHERE 
            S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.PACKING_SEQ <> :PACKING_SEQ
            AND S0.PRO_SCHEDULE_LINE = :PRO_SCHEDULE_LINE 
            AND S0.PRO_SCHEDULE_SEQ = :PRO_SCHEDULE_SEQ
            AND S0.PACKING_LINE = :PACKING_LINE
            AND S0.PACKING_MATERIAL_ID = :PACKING_MATERIAL_ID
            AND S0.DELETE_TIME IS NULL 
            AND S1.DETAIL_SEQ_START <= :DETAIL_SEQ_END
            AND S1.DETAIL_SEQ_END >= :DETAIL_SEQ_START
        `;
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        let result = await conn.execute(selectSQL, {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.COMPANY || user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.FIRM || user.FIRM },
            PACKING_SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PACKING_SEQ },
            PRO_SCHEDULE_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PRO_SCHEDULE_LINE },
            PRO_SCHEDULE_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: schedule.PRO_SCHEDULE_SEQ },
            PACKING_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PACKING_LINE },
            PACKING_MATERIAL_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: schedule.PACKING_MATERIAL_ID },
            DETAIL_SEQ_START: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_SEQ_START },
            DETAIL_SEQ_END: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DETAIL_SEQ_END },
        }, options);
        if (result.rows && result.rows.length) {
            obj.count = result.rows[0].COUNT;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'checkPackingDetailSeq', err);
        obj.error = err.toString();
    } finally {
        if (newConnection) {
            await conn.close();
        }
    }

    return obj;
}

//查詢生產排程
export async function getProScheduleByProductionLine(user, LINE, SEQ) {
    const obj = {
        schedules: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
        SELECT LINE, SEQ, PRD_PC, BATCH_NM, PRO_WT, LOT_NO, UKEY, CUST_PRD_PC, REPLACE(SILO, '-', '') AS SILO
        FROM AC.PRO_SCHEDULE
        WHERE 1 = 1
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            AND LINE = :LINE
            AND SEQ = :SEQ
            AND ROWNUM = 1 `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LINE },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + SEQ },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);

        if (result.rows && result.rows.length) {
            obj.schedules = result.rows;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getProSchedule', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//結束特定日期的包裝排程，並自動產生未完成的排程到隔天
export async function finishPackingScheduleByDay(user, packingDateStart) {
    const obj = {
        company: user.COMPANY,
        firm: user.FIRM,
        packingDate: packingDateStart,
        scheduleCount: 0,
        res: [],
        error: null,
    };


    let conn;
    try {
        if (!packingDateStart || !packingDateStart.match(/^\d{4}-\d{2}-\d{2}$/)) {
            throw new Error('日期格式錯誤，必須是 YYYY-MM-DD');
        }
        const tomorrow = moment(packingDateStart).add(1, 'days').format('YYYY-MM-DD');
        const packingDateEnd = packingDateStart;
        const completionRateReportResult = await PackingReport.getPackingCompletionRateReport(user, packingDateStart, packingDateEnd);
        if (completionRateReportResult.error) {
            throw new Error(packingDateStart + '查詢包裝排程失敗');
        }
        obj.scheduleCount = completionRateReportResult.completionRateReport.length;
        if (!obj.scheduleCount) {
            throw new Error(packingDateStart + '查無包裝排程');
        }

        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        console.log(getNowDatetimeString(), `檢查包裝過帳與重排 FIRM=${user.FIRM}, DATE=${packingDateStart}, 排程數=${obj.scheduleCount}`);
        //這個迴圈裏面會autoCommit，故處理每個排程時不是完整的資料庫交易
        for (let scheduleIndex = 0; scheduleIndex < obj.scheduleCount; scheduleIndex++) {
            const schedule = completionRateReportResult.completionRateReport[scheduleIndex];
            if (schedule.INV_SHTNO) {
                //已完成過帳
                continue;
            }

            let needNewSchedule = false;
            if (!schedule.PACKING_WEIGHT) {
                if ('包裝取消' !== schedule.PACKING_STATUS) {
                    // 沒有包裝重量，就直接改狀態為包裝取消
                    await this.updatePackingSchedule(conn, user, schedule.PACKING_SEQ, {
                        PACKING_STATUS: '包裝取消',
                    });
                    needNewSchedule = true;
                }
            } else if ('包裝中' === schedule.PACKING_STATUS) {
                const finishResult = await PackingWork.finishPacking(user, schedule, '強制結束');
                if (!finishResult.res) {
                    throw new Error(schedule.PACKING_SEQ + '包裝結束失敗');
                }
                needNewSchedule = (schedule.TARGET_WEIGHT - schedule.PACKING_WEIGHT) >= schedule.WEIGHT_PER_PALLET; //包裝重量已達標就不重排
            }

            if (needNewSchedule) {
                console.log(getNowDatetimeString(), `包裝排程需要重排 FIRM=${user.FIRM}, DATE=${packingDateStart}, SEQ=${schedule.PACKING_SEQ}, STATUS=${schedule.PACKING_STATUS}, 重量差=${schedule.TARGET_WEIGHT - schedule.PACKING_WEIGHT}, 棧板重=${schedule.WEIGHT_PER_PALLET}`);

                const newSchedule = {
                    ...schedule,
                    SEQ_START: schedule.MAX_DETAIL_SEQ_END ? schedule.MAX_DETAIL_SEQ_END + 1 : schedule.SEQ_START, //接續未完成的包裝序號
                    TARGET_WEIGHT: (schedule.TARGET_WEIGHT - schedule.PACKING_WEIGHT),
                    PACKING_SEQ: null,
                    PACKING_DATE: tomorrow,
                    PACKING_STATUS: null,
                    PACKING_SELECT: '續單包裝',
                };

                const saveResult = await savePackingSchedule(user, [newSchedule]);
                if (saveResult.error) {
                    throw new Error(schedule.PACKING_SEQ + '包裝重排失敗');
                }

                obj.res.push(needNewSchedule);
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'finishPackingScheduleByDay', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢過帳的INDATESEQ
export async function queryInDateSeq(user, invData, targetQty) {
    const obj = {
        res: [],
        targetQty: targetQty,
        totalQty: 0,
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        let selectSQL = `
        SELECT COMPANY, FIRM, DEPT, WAHS, INDATESEQ, 
            PRD_PC, PCK_KIND, PCK_NO, LOC, LOT_NO, 
            QTY
        FROM AC.LOCINV_D${invData.DEBUG ? '@ERPTEST' : ''}
        WHERE 1 = 1
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND WAHS = :WAHS
            AND PRD_PC = :PRD_PC
            AND LOT_NO = :LOT_NO
            AND LOC = :LOC
            AND PCK_KIND = :PCK_KIND
            AND PCK_NO = :PCK_NO
        ORDER BY IN_DATE ASC, LOT_NO ASC, INDATESEQ ASC
        `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.DEPT },
            WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.WAHS },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.PRD_PC },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.LOT_NO },
            LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.LOC },
            PCK_KIND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: invData.PCK_KIND },
            PCK_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: invData.PCK_NO },
        };
        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        //特殊規則: 換包
        if (invData.WAHS === invData.WAHS_IN && invData.LOC === invData.LOC_IN) {
            //忽略 包裝別
            selectSQL = selectSQL.replace('AND PCK_KIND = :PCK_KIND', '').replace('AND PCK_NO = :PCK_NO', 'AND PCK_NO LIKE :PCK_NO');
            delete params.PCK_KIND;
        }

        const result = await conn.execute(selectSQL, params, options);

        for (let i = 0; i < result.rows.length; i++) {
            const row = result.rows[i];
            if (row.QTY <= 0) {
                continue;
            }
            obj.totalQty += row.QTY;
            obj.res.push(row);
            if (obj.totalQty >= targetQty) {
                break;
            }
        }

        //找不到可扣的資料，就抓第一筆來扣(忽略負帳)
        if (!obj.res.length && result.rows.length) {
            obj.res.push(result.rows[0]);
        }

    } catch (err) {
        console.error(getNowDatetimeString(), 'queryInDateSeq', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//新增包裝API的呼叫LOG
export async function insertInvApiLog(conn, user, logType, rows) {
    const obj = {
        res: [],
        error: null,
    };

    //是否需要建立新連線
    const newConnection = !conn;
    try {
        if (newConnection) {
            conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        }

        const insertSQL = `
        INSERT INTO AC.PBTC_IOT_INV_API_LOG (
            CREATE_TIME, 
            LOG_TYPE, COMPANY, 
            FIRM, DEBUG, SHEET_ID, 
            SHTNO, DEPT, DEPT_IN, 
            WAHS, WAHS_IN, INVT_DATE, 
            PRD_PC, PCK_KIND, PCK_NO, 
            PRD_PC_IN, PCK_KIND_IN, PCK_NO_IN, 
            QTY, LOT_NO, LOTNO_IN, 
            LOC, LOC_IN, INDATESEQ, 
            CREATOR 
        ) 
        VALUES ( 
            :CREATE_TIME, 
            :LOG_TYPE, :COMPANY, 
            :FIRM, :DEBUG, :SHEET_ID, 
            :SHTNO, :DEPT, :DEPT_IN, 
            :WAHS, :WAHS_IN, :INVT_DATE, 
            :PRD_PC, :PCK_KIND, :PCK_NO, 
            :PRD_PC_IN, :PCK_KIND_IN, :PCK_NO_IN, 
            :QTY, :LOT_NO, :LOTNO_IN, 
            :LOC, :LOC_IN, :INDATESEQ, 
            :CREATOR
        ) `;

        const options = { outFormat: oracledb.OBJECT, autoCommit: false };

        const now = new Date();
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            //INSERT
            const params = {
                CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: now },
                LOG_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + logType },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.COMPANY ? row.COMPANY : ('' + user.COMPANY) },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.FIRM ? row.FIRM : ('' + user.FIRM) },
                DEBUG: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.DEBUG ? 1 : null },
                SHEET_ID: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SHEET_ID },
                SHTNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SHTNO },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT },
                DEPT_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.DEPT_IN },
                WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.WAHS },
                WAHS_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.WAHS_IN },
                INVT_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.INVT_DATE },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC },
                PCK_KIND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.PCK_KIND },
                PCK_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PCK_NO },
                PRD_PC_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PRD_PC_IN },
                PCK_KIND_IN: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.PCK_KIND_IN },
                PCK_NO_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.PCK_NO_IN },
                QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: row.QTY },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOT_NO },
                LOTNO_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOTNO_IN },
                LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOC },
                LOC_IN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.LOC_IN },
                INDATESEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.INDATESEQ },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.CREATOR },

            };
            // console.log(insertSQL, params);
            const result = await conn.execute(insertSQL, params, options);
            if (result.rowsAffected) {
                obj.res.push(result.lastRowid);
            } else {
                console.error(getNowDatetimeString(), 'insertInvApiLog 新增失敗');
            }
        }
        if (rows.length) {
            await conn.commit();
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'insertInvApiLog', err);
        obj.error = err.toString();
    } finally {
        if (newConnection) {
            await conn.close();
        }
    }

    return obj;
}

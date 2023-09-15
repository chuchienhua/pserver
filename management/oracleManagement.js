import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import moment from 'moment';
import oracledb from 'oracledb';

//查詢原料庫存追蹤表
export async function getMaterialInvTraceReport(user, reportDate) {
    const obj = {
        report: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //處理報表日期
        const reportDateMoment = moment(reportDate, 'YYYY-MM-DD');
        if (!reportDateMoment.isValid()) {
            throw new Error('報表日期不正確，必須是YYYY-MM-DD');
        }
        //時間範圍: 去年本月1日 ~ 下下個月最後1日
        reportDateMoment.subtract(1, 'years').startOf('month');
        const reportDateStart = reportDateMoment.toDate(); //本月1日
        reportDateMoment.add(1, 'years').endOf('month');
        const reportDateEnd0 = reportDateMoment.toDate(); //本月最後一日
        reportDateMoment.add(1, 'months').endOf('month');
        const reportDateEnd1 = reportDateMoment.toDate(); //下月最後一日
        reportDateMoment.add(1, 'months').endOf('month');
        const reportDateEnd2 = reportDateMoment.toDate(); //下下月最後一日

        const sql = `
        WITH TRN_ORD_MAT AS (
            SELECT R0.COMPANY, R0.FIRM, R0.MATERIAL, SUM(GREATEST(T1.STOCK_ONROAD, T1.UN_QTY - T1.LOCINV, 0) * R0.RATIO / 100) AS MATERIAL_USAGE_QTY
            FROM (
                SELECT T0.COMPANY, T0.FIRM, T0.PRD_PC, 
                    SUM(GET_LOCINV_D_QTY(T0.COMPANY, T0.FIRM, T0.PRD_PC, T0.PCK_KIND ,T0.PCK_NO, '')) AS LOCINV, --目前庫存
                    SUM(LC_PRD_STOCK_ONROAD(T0.COMPANY, T0.FIRM, T0.PRD_PC, T0.PCK_KIND, T0.PCK_NO)) AS STOCK_ONROAD, --在途庫存
                    SUM(UN_QTY) AS UN_QTY --訂單量
                FROM (
                    SELECT TRN_ORD.COMPANY, TRN_ORD.FIRM, TRN_ORD.PRD_PC, TRN_ORD.PCK_KIND, TRN_ORD.PCK_NO, TRN_ORD.UNIT, 
                        SUM(TRN_ORD.ORD_QTY - TRN_ORD.ACT_QTY) AS UN_QTY
                    FROM TRN_ORDH JOIN TRN_ORD
                        ON TRN_ORDH.ORD_NO=TRN_ORD.ORD_NO
                    WHERE 1 = 1
                        AND TRN_ORD.COMPANY = :COMPANY
                        AND TRN_ORD.FIRM = :FIRM
                        AND TRN_ORD.STATUS = '2'
                        AND TRN_ORD.ASS_DATE BETWEEN :REPORT_DATE_START AND :REPORT_DATE_END2
                    GROUP BY TRN_ORD.COMPANY, TRN_ORD.FIRM, TRN_ORD.PRD_PC, TRN_ORD.PCK_KIND, TRN_ORD.PCK_NO, TRN_ORD.UNIT
                ) T0
                GROUP BY T0.COMPANY, T0.FIRM, T0.PRD_PC
            ) T1 JOIN AC.PBTC_IOT_RECIPE R0
            ON T1.COMPANY = R0.COMPANY
                AND T1.FIRM = R0.FIRM
                AND T1.PRD_PC = R0.PRODUCT_NO
                AND R0.CREATE_TIME = ( SELECT MAX( CREATE_TIME ) 
                                    FROM PBTC_IOT_RECIPE 
                                    WHERE PRODUCT_NO = R0.PRODUCT_NO 
                                        AND COMPANY = R0.COMPANY 
                                        AND FIRM = R0.FIRM )
            GROUP BY R0.COMPANY, R0.FIRM, R0.MATERIAL
        )
        SELECT S0.MATERIAL_CODE, S0.MATERIAL_NOTE, 
            LC_MAT_STOCK(S0.COMPANY, S0.FIRM, S0.MATERIAL_CODE) AS MAT_STOCK,
            S0.MATER_NO, 
            GET_APPLY_SUMQTY(S0.COMPANY, S0.FIRM, S0.MATER_NO, :REPORT_DATE_START, :REPORT_DATE_END0, S0.DEPT_NO) AS BOOKING_STOCK_0, 
            GET_APPLY_SUMQTY(S0.COMPANY, S0.FIRM, S0.MATER_NO, :REPORT_DATE_START, :REPORT_DATE_END1, S0.DEPT_NO) AS BOOKING_STOCK_1, 
            GET_APPLY_SUMQTY(S0.COMPANY, S0.FIRM, S0.MATER_NO, :REPORT_DATE_START, :REPORT_DATE_END2, S0.DEPT_NO) AS BOOKING_STOCK_2, 
            S1.MATERIAL_USAGE_QTY
        FROM AC.PBTC_IOT_MAT_INV_TRACE_RPT S0
            LEFT JOIN TRN_ORD_MAT S1
                ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.MATERIAL_CODE = S1.MATERIAL
        WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
        ORDER BY S0.MATERIAL_ORDER ASC `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            REPORT_DATE_START: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: reportDateStart },
            REPORT_DATE_END0: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: reportDateEnd0 },
            REPORT_DATE_END1: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: reportDateEnd1 },
            REPORT_DATE_END2: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: reportDateEnd2 },
        };
        const options = { outFormat: oracledb.OBJECT };

        // console.log(sql, params);
        const result = await conn.execute(sql, params, options);

        if (result.rows && result.rows.length) {
            const map = new Map();
            result.rows.forEach(row => {
                const key = row.MATERIAL_CODE;
                let sumRow;
                if (map.has(key)) {
                    sumRow = map.get(key);
                } else {
                    sumRow = {
                        ...row,
                        MAT_STOCK: row.MAT_STOCK, //報表庫存
                        BOOKING_STOCK: 0, //已請購未到廠量
                        BOOKING_STOCK_0: 0, //已請購未到廠量 本月
                        BOOKING_STOCK_1: 0, //已請購未到廠量 下月
                        BOOKING_STOCK_2: 0, //已請購未到廠量 下下月
                    };
                    map.set(key, sumRow);
                }

                sumRow.BOOKING_STOCK = row.BOOKING_STOCK_2;
                sumRow.BOOKING_STOCK_0 = row.BOOKING_STOCK_0;
                sumRow.BOOKING_STOCK_1 = row.BOOKING_STOCK_1;
                sumRow.BOOKING_STOCK_2 = row.BOOKING_STOCK_2;

                sumRow.THIS_MONTH_STOCK = sumRow.MAT_STOCK + sumRow.BOOKING_STOCK_0 - row.MATERIAL_USAGE_QTY; //本月底預估庫存量
                sumRow.NEXT_MONTH_STOCK = sumRow.MAT_STOCK + sumRow.BOOKING_STOCK_1 - row.MATERIAL_USAGE_QTY; //下月底預估庫存量
                sumRow.NEXT_MONTH_STOCK2 = sumRow.MAT_STOCK + sumRow.BOOKING_STOCK_2 - row.MATERIAL_USAGE_QTY;//下下月底預估庫存量
            });

            obj.report = [...map.values()];
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMaterialInvTraceReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

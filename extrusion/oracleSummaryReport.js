import config from '../config.js';
import * as libs from '../libs.js';
import moment from 'moment';
import oracledb from 'oracledb';
import { createRequire } from 'module';
import * as Mailer from '../mailer.js';
const require = createRequire(import.meta.url);
const ExcelJS = require('exceljs');

export async function getDayReport(user, date) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
        SELECT 
            A.LINE,
            SUM(A.PRODUCTIVITY) AS PRODUCTIVITY, 
            SUM(A.WT_PER_SHIFT) AS STANDARD_PRODUCTIVITY,
            SUM(A.PRODUCTIVITY)/NULLIF(SUM(A.WT_PER_SHIFT), 0) AS AVABILITY_RATE,
            SUM(A.PRODUCTION_TIME) AS PRODUCTION_TIME,
            SUM(A.PRODUCTION_TIME)/24 AS AVABILITY_TIME,
            SUM(A.AMMETER) AS ELECTRICITY,
            SUM(A.AMMETER)/NULLIF(SUM(A.PRODUCTIVITY),0) AS ELECTRICITY_UNIT,
            HANDOVER.AI, HANDOVER.WD, HANDOVER.WASTEWATER, TRNOUT.SHIPMENT
        FROM PBTC_IOT_DAILY_REPORT A
            LEFT JOIN (
                SELECT 
                    SUM(AIR) AS AI, 
                    SUM(WATER_DEIONIZED) AS WD,
                    SUM(WATER_WASTE) AS WASTEWATER,
                    REPORT_DATE
                FROM PBTC_IOT_DAILY_HANDOVER
                WHERE REPORT_DATE = :TARGETDATE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                GROUP BY REPORT_DATE
            ) HANDOVER
                ON HANDOVER.REPORT_DATE = A.REPORT_DATE
            LEFT JOIN (
                SELECT SUM(OUT_QTY) AS SHIPMENT, OUT_DATE
                FROM TRN_OUT
                WHERE 1=1
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT_NO = :DEPT
                AND OUT_DATE = TO_DATE(:TARGETDATE,'YYYYMMDD')
                AND STATUS IN ('6','7')
                GROUP BY OUT_DATE
            ) TRNOUT
                ON TRNOUT.OUT_DATE = TO_DATE(A.REPORT_DATE, 'YYYYMMDD')
        WHERE A.COMPANY = :COMPANY
        AND A.FIRM = :FIRM
        AND A.DEPT = :DEPT
        AND A.REPORT_DATE = :TARGETDATE
        GROUP BY A.LINE,HANDOVER.AI, HANDOVER.WD, HANDOVER.WASTEWATER, TRNOUT.SHIPMENT
        ORDER BY A.LINE ASC `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            TARGETDATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + date },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getDayReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function exportDayRport(data) {
    const inputExcelPath_day = './src/extrusion/SummaryReport/生產日報_範本.xlsx';
    const workbook = new ExcelJS.Workbook();

    await workbook.xlsx.readFile(inputExcelPath_day);

    const dayReport = workbook.getWorksheet('PBTC生產日報_主管');
    const insertRow = ['PRODUCTIVITY', 'STANDARD_PRODUCTIVITY', 'AVABILITY_RATE', 'PRODUCTION_TIME', 'AVABILITY_TIME', 'ELECTRICITY', 'ELECTRICITY_UNIT',
        'WD', 'WASTEWATER', 'AI', 'SHIPMENT'];
    dayReport.insertRow(6, insertRow);
    dayReport.getRow(6).hidden = true;
    const targetData = dayReport.getRow(6).values;

    const dayData = {};

    let startRow = 7;
    let startCol = 'B';

    for (let i = 0; i < data.length; i++) {
        targetData.forEach(header => {
            dayData[header] = data[i][header];
            const cell = dayReport.getCell(`${startCol}${startRow}`);
            const col = dayReport.getColumn(`${startCol}`);
            col.width = 18;
            cell.value = dayData[header];
            startCol = String.fromCharCode(startCol.charCodeAt(0) + 1);
        });
        startCol = 'B';
        startRow += 1;
    }

    const buffer = await workbook.xlsx.writeBuffer();

    return buffer;
}

export async function getMonthReport(user, startDate, endDate) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
        SELECT 
            REPORT_DATE, A_PRODUCTIVITY, B_PRODUCTIVITY,
            C_PRODUCTIVITY, D_PRODUCTIVITY, E_PRODUCTIVITY,
            F_PRODUCTIVITY, G_PRODUCTIVITY, H_PRODUCTIVITY,
            K_PRODUCTIVITY, M_PRODUCTIVITY, N_PRODUCTIVITY,
            Q_PRODUCTIVITY, R_PRODUCTIVITY, S_PRODUCTIVITY,
            T_PRODUCTIVITY, ELECTRICITY,
            AI, WD, WASTEWATER, SHIPMENT,
            AI_UNIT, WD_UNIT, WW_UNIT, EL_UNIT,
            SUM(A_PRODUCTIVITY+B_PRODUCTIVITY+C_PRODUCTIVITY+D_PRODUCTIVITY+E_PRODUCTIVITY+
                F_PRODUCTIVITY+G_PRODUCTIVITY+ H_PRODUCTIVITY+K_PRODUCTIVITY+M_PRODUCTIVITY+
                N_PRODUCTIVITY+Q_PRODUCTIVITY+R_PRODUCTIVITY+S_PRODUCTIVITY+T_PRODUCTIVITY) AS DAY_PRODUCTIVITY,
            SUM(A_PRODUCTIVITY+B_PRODUCTIVITY+C_PRODUCTIVITY+D_PRODUCTIVITY+E_PRODUCTIVITY+
                F_PRODUCTIVITY+G_PRODUCTIVITY+ H_PRODUCTIVITY+K_PRODUCTIVITY+M_PRODUCTIVITY+
                N_PRODUCTIVITY+Q_PRODUCTIVITY+R_PRODUCTIVITY+S_PRODUCTIVITY+T_PRODUCTIVITY)/STANDARD_PRODUCTIVITY AS AVAIBILITY_RATE
        FROM (
            SELECT 
                A.REPORT_DATE,
                SUM(CASE WHEN LINE = 'A' THEN A.PRODUCTIVITY ELSE 0 END ) AS A_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'B' THEN A.PRODUCTIVITY ELSE 0 END ) AS B_PRODUCTIVITY, 
                SUM(CASE WHEN LINE = 'C' THEN A.PRODUCTIVITY ELSE 0 END ) AS C_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'D' THEN A.PRODUCTIVITY ELSE 0 END ) AS D_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'E' THEN A.PRODUCTIVITY ELSE 0 END ) AS E_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'F' THEN A.PRODUCTIVITY ELSE 0 END ) AS F_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'G' THEN A.PRODUCTIVITY ELSE 0 END ) AS G_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'H' THEN A.PRODUCTIVITY ELSE 0 END ) AS H_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'K' THEN A.PRODUCTIVITY ELSE 0 END ) AS K_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'M' THEN A.PRODUCTIVITY ELSE 0 END ) AS M_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'N' THEN A.PRODUCTIVITY ELSE 0 END ) AS N_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'Q' THEN A.PRODUCTIVITY ELSE 0 END ) AS Q_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'R' THEN A.PRODUCTIVITY ELSE 0 END ) AS R_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'S' THEN A.PRODUCTIVITY ELSE 0 END ) AS S_PRODUCTIVITY,
                SUM(CASE WHEN LINE = 'T' THEN A.PRODUCTIVITY ELSE 0 END ) AS T_PRODUCTIVITY,
                SUM(A.WT_PER_SHIFT) AS STANDARD_PRODUCTIVITY,
                SUM(A.PRODUCTION_TIME) AS PRODUCTION_TIME,
                SUM(A.AMMETER) AS ELECTRICITY,
                SUM(A.AMMETER)/NULLIF(SUM(A.PRODUCTIVITY),0) AS EL_UNIT,
                SUM(HANDOVER.AI)/NULLIF(SUM(A.PRODUCTIVITY),0) AS AI_UNIT,
                SUM(HANDOVER.WD)/NULLIF(SUM(A.PRODUCTIVITY), 0) AS WD_UNIT,
                SUM(HANDOVER.WASTEWATER)/NULLIF(SUM(A.PRODUCTIVITY), 0) AS WW_UNIT,
                HANDOVER.AI AS AI, HANDOVER.WD AS WD, HANDOVER.WASTEWATER AS WASTEWATER,
                TRNOUT.SHIPMENT AS SHIPMENT
            FROM PBTC_IOT_DAILY_REPORT A
                LEFT JOIN (
                    SELECT 
                        SUM(AIR) AS AI, 
                        SUM(WATER_DEIONIZED) AS WD,
                        SUM(WATER_WASTE) AS WASTEWATER,
                        REPORT_DATE
                    FROM PBTC_IOT_DAILY_HANDOVER
                    WHERE REPORT_DATE >= :STARTDATE
                    AND REPORT_DATE <= :ENDDATE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT
                    GROUP BY REPORT_DATE
                ) HANDOVER
                    ON HANDOVER.REPORT_DATE = A.REPORT_DATE
                LEFT JOIN (
                    SELECT SUM(OUT_QTY) AS SHIPMENT, OUT_DATE
                    FROM TRN_OUT
                    WHERE 1=1
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT_NO = :DEPT
                    AND OUT_DATE >= TO_DATE(:STARTDATE,'YYYYMMDD')
                    AND OUT_DATE <= TO_DATE(:ENDDATE, 'YYYYMMDD')
                    AND STATUS IN ('6','7')
                    GROUP BY OUT_DATE
                ) TRNOUT
                    ON TRNOUT.OUT_DATE = TO_DATE(A.REPORT_DATE, 'YYYYMMDD')
            WHERE A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
            AND A.REPORT_DATE >= :STARTDATE
            AND A.REPORT_DATE <= :ENDDATE
            GROUP BY A.REPORT_DATE, HANDOVER.AI, HANDOVER.WD, HANDOVER.WASTEWATER, TRNOUT.SHIPMENT
        )SUB
        GROUP BY  
        REPORT_DATE, A_PRODUCTIVITY, B_PRODUCTIVITY,
        C_PRODUCTIVITY, D_PRODUCTIVITY, E_PRODUCTIVITY,
        F_PRODUCTIVITY, G_PRODUCTIVITY, H_PRODUCTIVITY,
        K_PRODUCTIVITY, M_PRODUCTIVITY, N_PRODUCTIVITY,
        Q_PRODUCTIVITY, R_PRODUCTIVITY, S_PRODUCTIVITY,
        T_PRODUCTIVITY, WD, WASTEWATER, AI, ELECTRICITY, STANDARD_PRODUCTIVITY, SHIPMENT,
        AI_UNIT, WD_UNIT, WW_UNIT, EL_UNIT
        ORDER BY REPORT_DATE ASC `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            STARTDATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            ENDDATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        obj.res = result.rows;

    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getMonthReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function exportMonthRport(data) {
    const inputExcelPath_day = './src/extrusion/SummaryReport/生產月報_範本.xlsx';
    const workbook = new ExcelJS.Workbook();

    await workbook.xlsx.readFile(inputExcelPath_day);

    const dayReport = workbook.getWorksheet('Sheet1');
    const insertRow = ['DATE', 'LINE_A', 'LINE_B', 'LINE_C', 'LINE_D', 'LINE_E', 'LINE_F',
        'LINE_G', 'LINE_H', 'LINE_K', 'LINE_M', 'LINE_N', 'LINE_Q', 'LINE_R', 'LINE_S', 'LINE_T',
        'WW', 'AI', 'WD', 'EL', 'PRODUCTIVITY', 'AVABILITY_RATE', 'WW_UNIT', 'AI_UNIT', 'WD_UNIT', 'EL_UNIT','SHIPMENT'];
    dayReport.insertRow(5, insertRow);
    dayReport.getRow(5).hidden = true;
    const targetData = dayReport.getRow(5).values;

    const dayData = {};

    let startRow = 6;
    let startCol = 'A';

    for (let i = 0; i < data.length; i++) {
        targetData.forEach(header => {
            dayData[header] = data[i][header];
            if('[' === startCol) {
                startCol = 'AA';
            }
            const cell = dayReport.getCell(`${startCol}${startRow}`);
            const col = dayReport.getColumn(`${startCol}`);
            col.width = 18;
            if ('DATE' === header) {
                /*cell.value = Number(dayData[header]);*/
                cell.value = moment(dayData[header], 'YYYYMMDD').format('MM/DD/YY');
            } else {
                cell.value = dayData[header];
            }
            startCol = String.fromCharCode(startCol.charCodeAt(0) + 1);
        });
        startCol = 'A';
        startRow += 1;
    }

    const buffer = await workbook.xlsx.writeBuffer();

    return buffer;
}

export async function mailDayReport(user, date) {
    const obj = {
        res: [],
        error: null,
    };
    try {
        const yesterday = moment(date).subtract(1, 'days').format('YYYYMMDD');
        const filePath = './src/extrusion/SummaryReport/生產日報.xlsx';

        const result = await getDayReport(user, yesterday);
        const data = result.res;
        if (data.length) {
            const inputExcelPath_day = './src/extrusion/SummaryReport/生產日報_範本.xlsx';
            const workbook = new ExcelJS.Workbook();

            await workbook.xlsx.readFile(inputExcelPath_day);

            const dayReport = workbook.getWorksheet('PBTC生產日報_主管');
            const insertRow = ['PRODUCTIVITY', 'STANDARD_PRODUCTIVITY', 'AVABILITY_RATE', 'PRODUCTION_TIME', 'AVABILITY_TIME', 'ELECTRICITY', 'ELECTRICITY_UNIT',
                'WD', 'WASTEWATER', 'AI', 'SHIPMENT'];
            dayReport.insertRow(6, insertRow);
            dayReport.getRow(6).hidden = true;
            const targetData = dayReport.getRow(6).values;

            const dayData = {};

            let startRow = 7;
            let startCol = 'B';

            for (let i = 0; i < data.length; i++) {
                targetData.forEach(header => {
                    dayData[header] = data[i][header];
                    const cell = dayReport.getCell(`${startCol}${startRow}`);
                    const col = dayReport.getColumn(`${startCol}`);
                    col.width = 18;
                    cell.value = (null === dayData[header]) ? 0 : dayData[header];
                    startCol = String.fromCharCode(startCol.charCodeAt(0) + 1);
                });
                startCol = 'B';
                startRow += 1;
            }

            await workbook.xlsx.writeFile(filePath);
            await Mailer.summaryDayReport(user, filePath);
        }
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'mailDayReport', err);
        obj.error = err.toString();
    }

    return obj;
}

export async function mailMonthReport(user, date) {
    const obj = {
        res: [],
        error: null,
    };
    try {
        const startDate = moment(date).subtract(1, 'days').startOf('month').format('YYYYMMDD');
        const endDate = moment(date).subtract(1, 'days').endOf('month').format('YYYYMMDD');
        const filePath = './src/extrusion/SummaryReport/生產月報.xlsx';

        const result = await getMonthReport(user, startDate, endDate);
        const data = result.res;
        if (data.length) {
            const inputExcelPath_day = './src/extrusion/SummaryReport/生產月報_範本.xlsx';
            const workbook = new ExcelJS.Workbook();

            await workbook.xlsx.readFile(inputExcelPath_day);

            const dayReport = workbook.getWorksheet('Sheet1');
            const insertRow = ['REPORT_DATE', 'A_PRODUCTIVITY', 'B_PRODUCTIVITY', 'C_PRODUCTIVITY', 'D_PRODUCTIVITY', 'E_PRODUCTIVITY', 'F_PRODUCTIVITY',
                'G_PRODUCTIVITY', 'H_PRODUCTIVITY', 'K_PRODUCTIVITY', 'M_PRODUCTIVITY', 'N_PRODUCTIVITY', 'Q_PRODUCTIVITY', 'R_PRODUCTIVITY', 'S_PRODUCTIVITY', 'T_PRODUCTIVITY',
                'WASTEWATER', 'AI', 'WD', 'ELECTRICITY', 'DAY_PRODUCTIVITY', 'AVAIBILITY_RATE'];
            dayReport.insertRow(5, insertRow);
            dayReport.getRow(5).hidden = true;
            const targetData = dayReport.getRow(5).values;

            const dayData = {};

            let startRow = 6;
            let startCol = 'A';

            for (let i = 0; i < data.length; i++) {
                targetData.forEach(header => {
                    dayData[header] = data[i][header];
                    const cell = dayReport.getCell(`${startCol}${startRow}`);
                    const col = dayReport.getColumn(`${startCol}`);
                    col.width = 18;
                    if ('DATE' === header) {
                        cell.value = moment(dayData[header], 'YYYYMMDD').format('MM/DD/YY');
                    } else {
                        if (null === dayData[header]) {
                            cell.value = 0;
                        } else {
                            cell.value = dayData[header];
                        }
                    }
                    startCol = String.fromCharCode(startCol.charCodeAt(0) + 1);
                });
                startCol = 'A';
                startRow += 1;
            }

            await workbook.xlsx.writeFile(filePath);
            await Mailer.summaryMonthReport(user, filePath);
        }
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'mailMonthReport', err);
        obj.error = err.toString();
    }

    return obj;
}
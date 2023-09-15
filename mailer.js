import config from './config.js';
import oracledb from 'oracledb';
import * as extrusionStatistics from './extrusion/oracleStatistics.js';
import * as extrusionForm from './extrusion/oracleForm.js';
import axios from 'axios';
import XLSX from 'xlsx-js-style';
import fs from 'fs';
import FormData from 'form-data';
import moment from 'moment';
import { getNowDatetimeString } from './libs.js';

//取得所有寄信種類下的所有EMAIL
const getMailAddressee = async (sendKind, user) => {
    let conn;
    let mailList = [];
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT PERSON_FULL.NAME, PERSON_FULL.EMAIL_ADDR
            FROM PBTC_IOT_MAIL_INFO, PERSON_FULL
            WHERE PBTC_IOT_MAIL_INFO.PPS_CODE = PERSON_FULL.PPS_CODE
            AND PERSON_FULL.IS_ACTIVE IN ('A', 'T')
            AND PBTC_IOT_MAIL_INFO.SEND_KIND = :SEND_KIND
            AND PBTC_IOT_MAIL_INFO.COMPANY = :COMPANY
            AND PBTC_IOT_MAIL_INFO.FIRM = :FIRM
            ORDER BY DUTY_CODE, EMPLOYMENT_DATE `;
        const params = {
            SEND_KIND: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: sendKind },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        let result = await conn.execute(sql, params, options);
        mailList = result.rows.map(person => `${person.NAME} <${person.EMAIL_ADDR}>`).join(',');
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMailAddressee Error', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return mailList;
};

//寄信API
const SEND_MAIL_API = 'http://192.168.8.104:3501/sendMail';
const sendMail = (to, subject, html, cc, bcc, attachments = [], from = 'PBTc數位生管系統 <PBTc_IOT@ccpgp.com>') => {
    let axiosConfig = { proxy: false };

    const formData = new FormData();
    formData.append('to', to);
    formData.append('subject', subject);
    formData.append('html', html);
    formData.append('cc', cc || '');
    formData.append('bcc', bcc || '');
    formData.append('from', from);

    if (attachments.length) {
        axiosConfig['Content-Type'] = 'multipart/form-data';
        attachments.forEach(attachment => {
            //這邊要傳入檔案的路徑
            formData.append('attachments[]', fs.createReadStream(attachment));
        });
    } else {
        axiosConfig['Content-Type'] = 'application/x-www-form-urlencoded';
    }

    return axios.post(SEND_MAIL_API, formData, axiosConfig);
};

//當領料原料棧板尚未品檢合格時，寄信通知相關主管
export const pickingAlarm = async (lotNo, batchNo, line, sequence, material, qaResult, stage, user) => {
    const mailSendKind = 'QA_TEST';
    const stageTranslate = ('mixing' === stage) ? '拌粉' : '押出';

    let mailList = await getMailAddressee(mailSendKind, user);
    if ('1' === user.COMPANY) {
        mailList += ', 仁武廠生二部控制室及賀安共用信箱 <KPBT2@ccpgp.com>'; //測式期間暫時發送到共用信箱
    }

    const to = mailList;
    const subject = `PBTc${stageTranslate}原料備料/入料異常通知`;
    const html = `
        <h1>${stageTranslate}原料備料/入料異常通知</h1>
        <p>使用者${user.PPS_CODE}(${user.NAME})於${stageTranslate}:${line + sequence}排程時</p>
        <p>使用尚未品檢合格的原料:<code>${material}</code>;棧板編號:<code>${batchNo}</code>;Lot No:<code>${lotNo}</code></p>
        <p>當下品檢值:<code>${qaResult}</code></p>`;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//自動押出領繳異常寄信通知
export const autoExtrusionStorageAlarm = async (storageType, user) => {
    const mailSendKind = 'EXTR_AUTO';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = `PBTc自動押出${('picking' === storageType) ? '領料' : '繳庫'}異常通知`;
    const html = `
        <h1>自動押出${('picking' === storageType) ? '領料' : '繳庫'}異常通知</h1>`;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//料頭或前料不在排程啟動期間輸入異常
export const reworkAlarm = async (line, sequence, weight, user) => {
    const mailSendKind = 'REWORK';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc異常料頭前料產出通知';
    const html = `
        <h1>異常料頭前料產出通知</h1>
        <p>使用者${user.PPS_CODE}(${user.NAME})未在工令${line + sequence}排程生產期間內</p>
        <p>產出料頭前料${weight}Kg</p>`;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//料頭或前料不在排程啟動期間輸入異常
export const recipeAuthAlarm = async (user, functionName) => {
    const mailSendKind = 'RECIPE_AUTH';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc配方管理權限異常通知';
    const html = `
        <h1>配方管理權限異常通知</h1>
        <p>使用者${user.PPS_CODE}(${user.NAME})在公司${user.COMPANY}-廠別${user.FIRM} 為非管理員權限的狀況下，欲使用配方管理 ${functionName}功能 `;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};


//自動押出入料品質計算寄信通知
export const autoExtrusionQuality = async (date, qualityData, user) => {
    /*
        qualityData = [
            {LINE, SEQ, SCH_SEQ, PRD_PC, FEEDER_NO, MATERIAL, RATIO, PICK_WEIGHT, PICK_RATIO, PICK_DIFF},
            {LINE, SEQ, SCH_SEQ, PRD_PC, FEEDER_NO, MATERIAL, RATIO, PICK_WEIGHT, PICK_RATIO, PICK_DIFF},
            ...    
        ];
    */
    const filePath = 'tmp/tmp_quality.xlsx';
    const colNames = ['線別', '序號', '配方別', '成品簡碼', '入料機', '原料簡碼', '配方比例', '領料(%)', '差異(%)', '允差'];

    let wb = XLSX.utils.book_new();
    let ws = XLSX.utils.aoa_to_sheet([colNames]);
    ws['!cols'] = [5, 5, 8, 15, 8, 15, 10, 10, 10, 10].map(width => ({ wch: width })); //欄位寬度
    //let htmlString = `<p>${colNames.join(', ')}</p>`; //寄信不僅副上檔案，還要印html
    let htmlString = `
        <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="10" align="left">
            <thead bgcolor="#69EFE6">
                <tr>
                    <th>線別</th>
                    <th>序號</th>
                    <th>配方別</th>
                    <th>成品簡碼</th>
                    <th>入料機</th>
                    <th>原料簡碼</th>
                    <th>配方比例</th>
                    <th>領料(%)</th>
                    <th>差異(%)</th>
                    <th>允差(%)</th>
                </tr>
            </thead>
            <tbody> `;

    let rowCount = 0;
    qualityData.forEach((array, rowIndex) => {
        rowCount++;
        let cells = [array.LINE, array.SEQ, array.SCH_SEQ, array.PRD_PC, array.FEEDER_NO, array.MATERIAL, array.RATIO, array.PICK_RATIO, array.PICK_DIFF, array.TOLERANCE_RATIO];

        //將每個cell存到sheet中
        cells.forEach((col, colIndex) => {
            if (null === col) {
                col = '';
            }
            let value = col.toString().trim().replace(/\n/g, '\r\n');
            let cell = {
                v: value,
                t: 's',
                s: {
                    alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                },
            };

            if (0 === colIndex) {
                htmlString += '<tr>';
            }

            //處理差異(%)
            if (8 === colIndex) {
                let toleranceValue = (9 < cells.length) ? cells[9].toString().trim().replace(/\n/g, '\r\n') : '';
                if (toleranceValue <= Math.abs(value)) {
                    cell.s['fill'] = { fgColor: { rgb: 'FF0000' } };
                    cell.s['font'] = { color: { rgb: 'FFFFFF' } };
                    htmlString += `<td style="background-color: red; color: white">${value}</td>`;
                } else {
                    htmlString += `<td>${value}</td>`;
                }

                // htmlString += '</tr>';
            } else if (9 === colIndex) {
                htmlString += `<td>${value}</td></tr>`;
            } else {
                htmlString += `<td>${value}</td>`;
            }

            const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
            ws[addr] = cell;
        });
    });

    htmlString += '</tbody></table>';

    let lastAddr = XLSX.utils.encode_cell({ c: Math.max(colNames.length - 1, 0), r: Math.max(rowCount, 1) });
    ws['!ref'] = `A1:${lastAddr}`;

    XLSX.utils.book_append_sheet(wb, ws, 'report_1');
    XLSX.writeFile(wb, filePath, { type: 'file', bookType: 'xlsx' });

    const mailSendKind = 'EXTR_QUALITY';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc自動押出入料品質通知';
    const html = `
        <h3>押出入料品質每日報表</h3>
        <p>附件為日期${date}08:00自動執行，昨日之押出入料品質表</p>
        <p>${htmlString}</p>`;
    const cc = null;
    const bcc = null;
    const attachments = [filePath];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//交接紀錄表，儲存後自動寄信
export const saveHandoverForm = async (date, workShift, handoverData, user) => {
    const filePath = 'tmp/tmp_handoverform.xlsx';
    const colNames = [
        '線別', '負責人', '設備狀態', '停機事由', '序號', '生產規格', '生產經時', '押完經時',
        'SILO', '已生產量', '未包裝量', '停機時間', '停機待機累計', '備註',
    ];

    let wb = XLSX.utils.book_new();
    let ws = XLSX.utils.aoa_to_sheet([colNames]);
    ws['!cols'] = [6, 8, 8, 20, 5, 10, 10, 10, 8, 10, 10, 15, 12, 20].map(width => ({ wch: width })); //欄位寬度
    let htmlString = `
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" align="left">
        <thead bgcolor="#69EFE6">
            <tr>
                <th>線別</th><th>負責人</th><th>設備狀態</th><th>停機事由</th><th>序號</th><th>生產規格</th><th>生產經時</th><th>押完經時</th>
                <th>SILO</th><th>已生產量</th><th>未包裝量</th><th>停機時間</th><th>停機待機累計</th><th>備註</th>
            </tr>
        </thead>
        <tbody> `;

    let rowCount = 0;
    handoverData.forEach((array, rowIndex) => {
        rowCount++;
        let cells = [
            array.LINE, array.IC_NAME, array.EXTRUDER_STATUS, array.STOP_REASON, array.SEQ, array.PRD_PC, array.PRODUCTION_TIME, array.EXTRUDED_TIME,
            array.SILO, array.PRODUCTIVITY, array.UNPACK, array.STOP_TIME, array.TOTAL_STOP_TIME, array.NOTE,
        ];

        //將每個cell存到sheet中
        cells.forEach((col, colIndex) => {
            if (null === col) {
                col = '';
            } else if ('string' !== typeof col) {
                col = parseFloat(col).toFixed(2);
            }
            let value = col.toString().trim().replace(/\n/g, '\r\n');
            let cell = {
                v: value,
                t: 's',
                s: {
                    alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                },
            };

            if (0 === colIndex) {
                htmlString += '<tr>';
            }

            //處理設備狀態
            if (2 === colIndex) {
                if ('停俥' === value) {
                    cell.s['font'] = { color: { rgb: 'FF0000' } };
                    htmlString += `<td style="background-color: red; color: white">${value}</td>`;
                } else {
                    htmlString += `<td>${value}</td>`;
                }

            } else if (11 === colIndex) {
                //處理停機時間
                if (value.length) {
                    cell.s['font'] = { color: { rgb: 'FF0000' } };
                    htmlString += `<td style="background-color: red; color: white">${value}</td>`;
                } else {
                    htmlString += `<td>${value}</td>`;
                }

            } else if (13 === colIndex) {
                htmlString += `<td>${value}</td>`;
                htmlString += '</tr>';

            } else {
                htmlString += `<td>${value}</td>`;
            }

            const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
            ws[addr] = cell;
        });
    });

    htmlString += '</tbody></table>';

    let lastAddr = XLSX.utils.encode_cell({ c: Math.max(colNames.length - 1, 0), r: Math.max(rowCount, 1) });
    ws['!ref'] = `A1:${lastAddr}`;

    XLSX.utils.book_append_sheet(wb, ws, 'report_1');
    XLSX.writeFile(wb, filePath, { type: 'file', bookType: 'xlsx' });

    const mailSendKind = 'HANDOVER';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc儲存交接紀錄表通知';
    const html = `
        <h3>交接紀錄表</h3>
        <p>附件為日期${date}${workShift}班，由${user.NAME}儲存交接紀錄表通知</p>
        <p>${htmlString}</p>`;
    const cc = null;
    const bcc = null;
    const attachments = [filePath];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//生產日報表，儲存後自動寄信
export const saveDailyForm = async (date, workShift, dailyData, handoverNote, user) => {
    const filePath = 'tmp/tmp_dailyForm.xlsx';
    const colNames = [
        '線別', '班別', '序號', '產品規格', '標準押出量', '實際押出量', '生產經時', '標準產量', '實際產量',
        '通訊異常', '停機時間', '停機-準備', '停機-等待', '停機-清機', '停機-現場排除', '停機-工務維修', '停機-計畫性停機', '停機-其他',
        '領班', '主控', '料頭-開關機', '料頭-斷條', '料頭-設備異常', '總料頭重', '前料量', '降載及停機原因',
    ];

    let wb = XLSX.utils.book_new();
    let ws = XLSX.utils.aoa_to_sheet([colNames]);
    ws['!cols'] = [
        5, 5, 5, 10, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 20
    ].map(width => ({ wch: width })); //欄位寬度
    let htmlString = `
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" align="left">
        <thead bgcolor="#69EFE6">
            <tr>
                <th>線別</th><th>班別</th><th>序號</th><th>產品規格</th><th>標準押出量</th><th>實際押出量</th><th>生產經時</th><th>標準產量</th><th>實際產量</th>
                <th>通訊異常</th><th>停機時間</th><th>停機-準備</th><th>停機-等待</th><th>停機-清機</th><th>停機-現場排除</th><th>停機-工務維修</th>
                <th>停機-計畫性停機</th><th>停機-其他</th>
                <th>領班</th><th>主控</th><th>料頭-開關機</th><th>料頭-斷條</th><th>料頭-設備異常</th><th>總料頭重</th><th>前料量</th><th>降載及停機原因</th>
            </tr>
        </thead>
        <tbody> `;

    let rowCount = 0;
    dailyData.forEach((array, rowIndex) => {
        rowCount++;
        let cells = [
            array.LINE, array.WORK_SHIFT, array.SEQ, array.PRD_PC, array.WT_PER_HR, array.WT_PER_HR_ACT, array.PRODUCTION_TIME, array.WT_PER_SHIFT, array.PRODUCTIVITY,
            array.DISCONNECT_TIME, array.STOP_TIME, array.STOP_1, array.STOP_2, array.STOP_3, array.STOP_4, array.STOP_5, array.STOP_6, array.STOP_7,
            array.IC_NAME, array.CONTROLLER_NAME, array.WEIGHT_RESTART, array.WEIGHT_BREAK, array.WEIGHT_ABNORMAL, array.WEIGHT_SCRAP, array.WEIGHT_HEAD, array.NOTE,
        ];

        //將每個cell存到sheet中
        cells.forEach((col, colIndex) => {
            if (null === col) {
                col = '';
            } else if ('string' !== typeof col && 2 !== colIndex) {
                col = parseFloat(col).toFixed(2);
            }
            let value = col.toString().trim().replace(/\n/g, '\r\n');
            let cell = {
                v: value,
                t: 's',
                s: {
                    alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                },
            };

            if (0 === colIndex) {
                htmlString += '<tr>';
            }

            if (25 === colIndex) {
                htmlString += `<td>${value}</td>`;
                htmlString += '</tr>';

            } else {
                htmlString += `<td>${value}</td>`;
            }

            const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
            ws[addr] = cell;
        });
    });

    htmlString += '</tbody></table>';

    let lastAddr = XLSX.utils.encode_cell({ c: Math.max(colNames.length - 1, 0), r: Math.max(rowCount, 1) });
    ws['!ref'] = `A1:${lastAddr}`;

    XLSX.utils.book_append_sheet(wb, ws, 'report_1');
    XLSX.writeFile(wb, filePath, { type: 'file', bookType: 'xlsx' });

    const mailSendKind = 'DAILY_FORM';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc儲存生產日報表通知';
    const html = `
        <h3>生產日報表</h3>
        <p>附件為日期${date}${workShift}班，由${user.NAME}儲存生產日報表通知</p>
        <p>交接事項如下</p>
        <p>${handoverNote}</p>
        <p>${htmlString}</p>`;
    const cc = null;
    const bcc = null;
    const attachments = [filePath];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//生產日報表_主管
export const summaryDayReport = async (user, filePath ) => {
    const mailSendKind = 'SummaryReport';
    let mailList = await getMailAddressee(mailSendKind, user);
    // let mailList = '邱亮智 <liangchih_chiu@ccpgp.com>';

    const to = mailList;
    const subject = 'PBTc生產日報表';
    const html = `
        <h3>生產日報表</h3>
        <p>附件為日期${moment(new Date()).subtract(1, 'days').format('YYYY-MM-DD')}的生產日報表</p>`;
    const cc = null;
    const bcc = null;
    const attachments = [filePath];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//生產月報表
export const summaryMonthReport = async (user, filePath) => {
    const mailSendKind = 'SummaryReport';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc生產月報表';
    const html = `
        <h3>生產月報表</h3>
        <p>附件為${moment(new Date()).subtract(1, 'days').month() + 1}月的生產月報表</p>`;
    const cc = null;
    const bcc = null;
    const attachments = [filePath];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//將scheduleMonitor抓到完成的工令附上網址url
export const scheduleMonitorAlert = async (finishedSchedules, user) => {
    const mailSendKind = 'STATISTICS';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc自動監控生產排程完成通知';
    let html = `<h1>自動監控生產排程完成通知</h1><p>時間${moment(new Date()).format('MM-DD HH:mm')}</p>`;
    for (const schedule of finishedSchedules) {
        html += `
            <p>${schedule.LINE}-${schedule.SEQ}已生產完成，趨勢圖
            <a href="https://tpiot.ccpgp.com/pbtc/extrusion?tabName=extruder&line=${schedule.LINE}&seq=${schedule.SEQ}&prd=${schedule.PRD_PC}">
            點此<a/>查看</p>`;
    }
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//押出完成仍有成品簡碼殘包未使用通知
export const alertNotUsingRemain = async (user, line, sequence, productNo, endTime, lono, weight, lotNo) => {

    const mailSendKind = 'REMAINBAG';
    let mailList = await getMailAddressee(mailSendKind, user);

    const to = mailList;
    const subject = 'PBTc殘包未使用通知(押出排程已完成)';
    let html = `
        <h1>殘包未使用通知(押出排程已完成)</h1>
        <p>排程${line}-${sequence}於${moment().format('YYYY-MM-DD HH:mm:ss')}輸入押出生產完成</p>
        <p>所輸入時間為${moment(endTime).format('YYYY-MM-DD HH:mm:ss')}</p>
        <p>輸入押出生產完成時間時，仍有未使用<code>${productNo}</code>殘包</p>
        <p>殘包格位<code>${lono}</code>，重量<code>${weight}</code>，批號<code>${lotNo}</code></p>`;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//寄送存放過久的殘包資訊
export const alertRemainBagStock = async (user, remainBagInfo) => {

    const mailSendKind = 'REMAINBAG';
    let mailList = await getMailAddressee(mailSendKind, user);

    // let mailList = '邱亮智 <liangchih_chiu@ccpgp.com>';
    const to = mailList;
    const subject = 'PBTc殘包存放過久通知';
    let html = `
        <h1>殘包存放過久通知</h1>
        <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
                <thead bgcolor="#69EFE6">
                    <tr><th>規格</th><th>批號</th><th>重量(KG)</th><th>格位</th><th>入庫日期</th><th>存放天數</th></tr>
                </thead>
                <tbody> `;
    for (const bagInfo of remainBagInfo) {
        html += `
                <tr>
                    <td>${bagInfo.PRD_PC}</td>
                    <td>${bagInfo.LOT_NO}</td>
                    <td>${bagInfo.WEIGHT}</td>
                    <td>${bagInfo.LONO}</td>
                    <td>${moment(bagInfo.INV_DATE).format('YYYY-MM-DD HH:mm:ss')}</td>
                    <td>${moment().diff(bagInfo.INV_DATE, 'days')}</td>
                </tr> `;
    }
    html += '</tbody></table>';
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};

//停機分析與個人績效週/月報
export const weeklyMonthlyReport = async (type, user) => {
    const startDate = ('weekly' === type) ? moment().subtract(1, 'week').format('YYYYMMDD') : moment().subtract(1, 'month').format('YYYYMMDD');
    const todayDate = moment().format('YYYYMMDD');
    const month = moment().subtract(1, 'day').format('YYYYMM');

    const shutDownReport = await extrusionStatistics.getShutdown(startDate, todayDate, user);
    let htmlString = `
    <h3>停機項目分析</h3>
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
        <thead bgcolor="#69EFE6">
            <tr><th>日期</th><th>準備</th><th>等待</th><th>清機</th><th>現場排除</th><th>公務維修</th><th>計畫性停機</th><th>其他</th><th>合計停機</th></tr>
        </thead>
        <tbody> `;

    shutDownReport.res.forEach(row => {
        htmlString += `
        <tr>
            <td>${moment(row.REPORT_DATE).format('YYYY-MM-DD')}</td>
            <td>${parseFloat(row.STOP_1).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_2).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_3).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_4).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_5).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_6).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_7).toFixed(2)}</td>
            <td>${parseFloat(row.STOP_TIME).toFixed(2)}</td>
        </tr>`;
    });
    htmlString += '</tbody></table>';

    const performanceReport = await extrusionForm.getCrewPerformance(startDate, todayDate, user);
    htmlString += `
    <h3>領班個人績效</h3>
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
        <thead bgcolor="#69EFE6">
            <tr><th>領班</th><th>產量(MT)</th><th>料頭(kg)</th><th>不良率(%)</th><th>生產時數(hr)</th><th>停機時數(hr)</th><th>停機率(%)</th><th>生產力</th></tr>
        </thead>
        <tbody></tbody>`;
    performanceReport.ic.forEach(row => {
        htmlString += `
        <tr>
            <td>${row.IC_NAME}</td>
            <td>${parseFloat(row.PRODUCTIVITY).toFixed(1)}</td>
            <td>${parseFloat(row.SCRAP_WEIGHT).toFixed(1)}</td>
            <td>${parseFloat(row.SCRAP_WEIGHT / (row.PRODUCTIVITY * 1000)).toFixed(2)}</td>
            <td>${parseFloat(row.PRODUCTION_TIME).toFixed(1)}</td>
            <td>${parseFloat(row.STOP_TIME).toFixed(1)}</td>
            <td>${parseFloat(row.STOP_TIME / row.PRODUCTION_TIME).toFixed(3)}</td>
            <td>${parseFloat(row.PRODUCTIVITY / row.PRODUCTION_TIME).toFixed(3)}</td>
        </tr>`;
    });
    htmlString += '</tbody></table>';

    htmlString += `
    <h3>主控個人績效</h3>
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
        <thead bgcolor="#69EFE6">
            <tr><th>主控</th><th>產量(MT)</th><th>料頭(kg)</th><th>不良率(%)</th><th>生產時數(hr)</th><th>停機時數(hr)</th><th>停機率(%)</th><th>生產力</th></tr>
        </thead>
        <tbody></tbody>`;
    performanceReport.controller.forEach(row => {
        htmlString += `
        <tr>
            <td>${row.CONTROLLER_NAME}</td>
            <td>${parseFloat(row.PRODUCTIVITY).toFixed(1)}</td>
            <td>${parseFloat(row.SCRAP_WEIGHT).toFixed(1)}</td>
            <td>${parseFloat(row.SCRAP_WEIGHT / (row.PRODUCTIVITY * 1000)).toFixed(2)}</td>
            <td>${parseFloat(row.PRODUCTION_TIME).toFixed(1)}</td>
            <td>${parseFloat(row.STOP_TIME).toFixed(1)}</td>
            <td>${parseFloat(row.STOP_TIME / row.PRODUCTION_TIME).toFixed(3)}</td>
            <td>${parseFloat(row.PRODUCTIVITY / row.PRODUCTION_TIME).toFixed(3)}</td>
        </tr>`;
    });
    htmlString += '</tbody></table>';

    const productSummary = await extrusionForm.getProductionSummary('' + month, user);

    htmlString += `
    <h3>生產總表</h3>
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
        <thead bgcolor="#69EFE6">
            <tr><th>日期</th><th>生產量(MT)</th><th>料頭產出(MT)</th><th>料頭占比(%)</th><th>A</th><th>B</th><th>C</th><th>D</th><th>E</th><th>F</th><th>G</th><th>H</th><th>K</th><th>M</th><th>N</th><th>Q</th><th>R</th><th>T</th></tr>
        </thead>
        <tbody></tbody>`;

    let totalProductivity = 0;
    let monthScrap = 0;
    let lineProductivity = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0, 'F': 0, 'G': 0, 'H': 0, 'K': 0, 'M': 0, 'N': 0, 'Q': 0, 'R': 0, 'T': 0 };
    let lineProductivityGoal = { 'A': 220, 'B': 280, 'C': 900, 'D': 800, 'E': 300, 'F': 300, 'G': 160, 'H': 260, 'K': 250, 'M': 230, 'N': 230, 'Q': 230, 'R': 300, 'T': 300 };
    let dayProductivity = 0;
    let dayScrap = 0;
    let totalRecord = [];
    const productGoal = productSummary.goal;

    productSummary.res.forEach(row => {
        dayProductivity += row.PRODUCTIVITY;
        totalProductivity += row.PRODUCTIVITY;
        dayScrap += row.WEIGHT_SCRAP;

        let weightScrap = Number(parseFloat(row.WEIGHT_SCRAP)).toFixed(2);
        monthScrap += Number(weightScrap);

        lineProductivity[row.LINE] += row.PRODUCTIVITY;
        if (row.LINE === 'T') {
            totalRecord.push(row.REPORT_DATE, parseFloat(dayProductivity).toFixed(2), parseFloat(dayScrap).toFixed(2));
            dayProductivity = 0;
            dayScrap = 0;
        }
    });
    htmlString += `
    <tr>
        <td>生產目標</td><td>${parseFloat(productSummary.goal).toFixed(2)}</td>
        <td>15.00</td><td>0.50</td><td>220.00</td><td>280.00</td><td>900.00</td><td>800.00</td><td>300.00</td><td>300.00</td><td>160.00</td><td>260.00</td><td>250.00</td><td>230.00</td><td>230.00</td><td>230.00</td><td>300.00</td><td>300.00</td>
    </tr>
    <tr>
        <td>累積實績</td><td>${parseFloat(totalProductivity).toFixed(2)}</td><td>${parseFloat(monthScrap).toFixed(2)}</td>
        <td>${parseFloat(monthScrap * 0.001 * 100 / totalProductivity).toFixed(2)}</td>`;
    for (const value of Object.values(lineProductivity)) {
        if (!isNaN(value)) {
            htmlString += `<td>${parseFloat(value).toFixed(3)}</td>`;
        }
    }

    htmlString += `
    </tr>
    <tr>
        <td>達成率(%)</td><td>${parseFloat(100 * totalProductivity / productSummary.goal).toFixed(2)}</td><td>${parseFloat(100 * monthScrap / 15).toFixed(2)}</td>
        <td>${parseFloat(monthScrap * 0.001 * 100 / totalProductivity * 0.5).toFixed(2)}</td>`;
    for (const [key, value] of Object.entries(lineProductivity)) {
        let lineGoal = lineProductivityGoal[key];
        if (!isNaN(value) && typeof (lineGoal) !== undefined) {
            htmlString += `<td>${parseFloat(100 * value / lineGoal).toFixed(3)}</td>`;
        }
    }
    htmlString += '</tr>';

    let dateProductivity = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0, 'F': 0, 'G': 0, 'H': 0, 'K': 0, 'M': 0, 'N': 0, 'Q': 0, 'R': 0, 'T': 0 };
    let i = 0;
    productSummary.res.forEach(row => {
        dateProductivity[row.LINE] = row.PRODUCTIVITY;
        if (row.LINE === 'T') {
            let reportDate = totalRecord[i];
            let productivity = totalRecord[i + 1];
            let weightScrap = totalRecord[i + 2];
            htmlString += `
                <tr>
                    <td>${reportDate}</td>
                    <td>${productivity}</td>
                    <td>${weightScrap}</td>
                    <td>${parseFloat(weightScrap / productivity).toFixed(2)}</td>`;
            for (const [key, value] of Object.entries(dateProductivity)) {
                if (key !== 'S') {
                    htmlString += `<td>${parseFloat(value).toFixed(2)}</td>`;
                }
            }
            for (var key in dateProductivity) {
                dateProductivity[key] = 0;
            }
            i += 3;

        }
    });

    htmlString += `
    </tr>
    </tbody></table>`;

    const availability = await extrusionForm.getLineAvability('' + month, user);
    const availabilityArray = [];
    let availabilityWT = {
        'A': 0, 'B': 0, 'C': 0, 'D': 0,
        'E': 0, 'F': 0, 'G': 0, 'H': 0, 'K': 0,
        'M': 0, 'N': 0, 'Q': 0, 'R': 0, 'T': 0,
    };
    let availabilityTime = {
        'A': 0, 'B': 0, 'C': 0, 'D': 0,
        'E': 0, 'F': 0, 'G': 0, 'H': 0, 'K': 0,
        'M': 0, 'N': 0, 'Q': 0, 'R': 0, 'T': 0
    };

    htmlString += `
    <h3>稼動率</h3>
    <table rules="all" bordercolor="#4d4c4d" border="1" bgcolor="#FFFFFF" cellpadding="5" width=100%>
        <thead bgcolor="#69EFE6">
            <tr>
                <th>日期</th>
                <th>A產量<br>稼動率</th><th>A經時<br>稼動率</th>
                <th>B產量<br>稼動率</th><th>B經時<br>稼動率</th>
                <th>C產量<br>稼動率</th><th>C經時<br>稼動率</th>
                <th>D產量<br>稼動率</th><th>D經時<br>稼動率</th>
                <th>E產量<br>稼動率</th><th>E經時<br>稼動率</th>
                <th>F產量<br>稼動率</th><th>F經時<br>稼動率</th>
                <th>G產量<br>稼動率</th><th>G經時<br>稼動率</th>
                <th>H產量<br>稼動率</th><th>H經時<br>稼動率</th>
                <th>K產量<br>稼動率</th><th>K經時<br>稼動率</th>
                <th>M產量<br>稼動率</th><th>M經時<br>稼動率</th>
                <th>N產量<br>稼動率</th><th>N經時<br>稼動率</th>
                <th>Q產量<br>稼動率</th><th>Q經時<br>稼動率</th>
                <th>R產量<br>稼動率</th><th>R經時<br>稼動率</th>
                <th>T產量<br>稼動率</th><th>T經時<br>稼動率</th>
            </tr>
        </thead>
        <tbody></tbody>`;

    //稼動率附檔

    let appendArray = [];
    let lastDate = '';
    let dateRow = {};
    availability.res.forEach(row => {
        if (lastDate !== row.REPORT_DATE) {
            dateRow = { DATE: row.REPORT_DATE };
            appendArray.push(dateRow);
        }
        lastDate = row.REPORT_DATE;
        dateRow[`${row.LINE}_WT`] = row.AVABILITY_WT || 0;
        dateRow[`${row.LINE}_TIME`] = row.AVABILITY_TIME || 0;
        availabilityWT[row.LINE] = row.AVABILITY_WT;
        availabilityTime[row.LINE] = row.AVABILITY_TIME;
        if (row.LINE === 'T') {
            htmlString += `
            <tr>
                <td>${row.REPORT_DATE}</td>`;
            availabilityArray.push({ 'DATE': row.REPORT_DATE });
            for (const [key, value] of Object.entries(availabilityWT)) {
                let availability_Time = availabilityTime[key];
                if (key !== 'S') {
                    if (!value && typeof (value) !== undefined && value !== 0) {
                        htmlString += '<td>NA</td>';
                        if (availability_Time === '') {
                            htmlString += '<td>NA</td>';
                        } else {
                            htmlString += `<td>${parseFloat(availability_Time).toFixed(2)}</td>`;
                        }
                    } else {
                        htmlString += `<td>${parseFloat(value).toFixed(2)}</td>`;
                        if (availability_Time === '') {
                            htmlString += '<td>NA</td>';
                        } else {
                            htmlString += `<td>${parseFloat(availability_Time).toFixed(2)}</td>`;
                        }
                    }
                }
            }
        }
    });

    htmlString += `</tr>
    </tbody></table>`;

    const filePath = 'tmp/tmp_availability.xlsx';
    const colNames_availability = [
        '日期', 'A產量稼動率', 'A經時稼動率', 'B產量稼動率', 'B經時稼動率', 'C產量稼動率', 'C經時稼動率', 'D產量稼動率', 'D經時稼動率',
        'E產量稼動率', 'E經時稼動率', 'F產量稼動率', 'F經時稼動率', 'G產量稼動率', 'G經時稼動率', 'H產量稼動率', 'H經時稼動率', 'K產量稼動率', 'K經時稼動率',
        'M產量稼動率', 'M經時稼動率', 'N產量稼動率', 'N經時稼動率', 'Q產量稼動率', 'Q經時稼動率', 'R產量稼動率', 'R經時稼動率', 'T產量稼動率', 'T經時稼動率',
    ];
    let wb_ava = XLSX.utils.book_new();
    let ws_ava = XLSX.utils.aoa_to_sheet([colNames_availability]);
    ws_ava['!cols'] = [
        15, 15, 15, 15, 15, 15, 15, 15, 15,
        15, 15, 15, 15, 15, 15, 15, 15, 15,
        15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15
    ].map(width => ({ wch: width })); //欄位寬度
    let rowCount = 0;
    appendArray.forEach((array, rowIndex) => {
        rowCount++;
        let cells_ava = [
            array.DATE, array.A_WT, array.A_TIME, array.B_WT, array.B_TIME, array.C_WT, array.C_TIME, array.D_WT, array.D_TIME,
            array.E_WT, array.E_TIME, array.F_WT, array.F_TIME, array.G_WT, array.G_TIME, array.H_WT, array.H_TIME, array.K_WT,
            array.K_TIME, array.M_WT, array.M_TIME, array.N_WT, array.N_TIME, array.Q_WT, array.Q_TIME, array.R_WT, array.R_TIME,
            array.T_WT, array.T_TIME
        ];

        cells_ava.forEach((col, colIndex) => {
            if (null === col) {
                col = '';
            } else if ('string' !== typeof col) {
                if (0 === col || null === col) {
                    col = 'NA';
                } else {
                    col = parseFloat(col).toFixed(2);
                }
            }
            let value = col.toString().trim().replace(/\n/g, '\r\n');
            let cell = {
                v: value,
                t: 's',
                s: {
                    alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                },
            };

            const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
            ws_ava[addr] = cell;
        });
    });

    let lastAddr = XLSX.utils.encode_cell({ c: Math.max(colNames_availability.length - 1, 0), r: Math.max(rowCount, 1) });
    ws_ava['!ref'] = `A1:${lastAddr}`;

    XLSX.utils.book_append_sheet(wb_ava, ws_ava, 'report_1');
    XLSX.writeFile(wb_ava, filePath, { type: 'file', bookType: 'xlsx' });

    //生產總表附檔
    let appendArray_pro = [
        {
            DATE: '生產目標', PRODUCTIVITY: productGoal, SCRAP: 15, SCRAP_RATIO: 0.5,
            A: 220, B: 280, C: 900, D: 800, E: 300, F: 300, G: 160, H: 260,
            K: 250, M: 230, N: 230, Q: 230, R: 300, T: 300,
        },
        {
            DATE: '累計實績', PRODUCTIVITY: parseFloat(totalProductivity).toFixed(2), SCRAP: parseFloat(monthScrap).toFixed(2), SCRAP_RATIO: parseFloat(monthScrap).toFixed(2),
            A: lineProductivity['A'], B: lineProductivity['B'], C: lineProductivity['C'], D: lineProductivity['D'], E: lineProductivity['E'], F: lineProductivity['F'],
            G: lineProductivity['G'], H: lineProductivity['H'], K: lineProductivity['K'], M: lineProductivity['M'], N: lineProductivity['N'], Q: lineProductivity['Q'],
            R: lineProductivity['R'], T: lineProductivity['T'],
        },
        {
            DATE: '達成率(%)', PRODUCTIVITY: parseFloat(100 * totalProductivity / productSummary.goal).toFixed(2), SCRAP: parseFloat(100 * monthScrap / 15).toFixed(2),
            SCRAP_RATIO: parseFloat(monthScrap * 0.001 * 100 / totalProductivity * 0.5).toFixed(2),
            A: 100 * lineProductivity['A'] / lineProductivityGoal['A'], B: 100 * lineProductivity['B'] / lineProductivityGoal['B'], C: 100 * lineProductivity['C'] / lineProductivityGoal['C'],
            D: 100 * lineProductivity['D'] / lineProductivityGoal['D'], E: 100 * lineProductivity['E'] / lineProductivityGoal['E'], F: 100 * lineProductivity['F'] / lineProductivityGoal['F'],
            G: 100 * lineProductivity['G'] / lineProductivityGoal['G'], H: 100 * lineProductivity['H'] / lineProductivityGoal['H'],
            K: 100 * lineProductivity['K'] / lineProductivityGoal['K'], M: 100 * lineProductivity['M'] / lineProductivityGoal['M'], N: 100 * lineProductivity['N'] / lineProductivityGoal['N'],
            Q: 100 * lineProductivity['Q'] / lineProductivityGoal['Q'], R: 100 * lineProductivity['R'] / lineProductivityGoal['R'], T: 100 * lineProductivity['T'] / lineProductivityGoal['T'],
        },
    ];
    let lastDate_pro = '';
    let dateRow_pro = { SCRAP: 0, SCRAP_RATIO: 0 };
    productSummary.res.forEach(row => {
        if (lastDate_pro !== row.REPORT_DATE) {
            dateRow_pro = {
                DATE: row.REPORT_DATE,
                PRODUCTIVITY: `=SUM(E${appendArray_pro.length + 2}:Z${appendArray_pro.length + 2})`,
                SCRAP: row.WEIGHT_SCRAP,
                SCRAP_RATIO: `=ROUND(C${appendArray_pro.length + 2}/B${appendArray_pro.length + 2}, 2)`,
            };
            appendArray_pro.push(dateRow_pro);
        }
        lastDate_pro = row.REPORT_DATE;
        dateRow_pro[row.LINE] = row.PRODUCTIVITY;
        dateRow_pro['SCRAP'] = dateRow_pro['SCRAP'] + row.WEIGHT_SCRAP;
    });
    const filePath_product = 'tmp/tmp_productSummary.xlsx';
    const colNames_productSummary = [
        '日期', '生產量(MT)', '料頭產出(MT)', '料頭占比(%)', 'A', 'B', 'C', 'D',
        'E', 'F', 'G', 'H', 'K', 'M', 'N', 'Q', 'R', 'T',
    ];
    let wb_pro = XLSX.utils.book_new();
    let ws_pro = XLSX.utils.aoa_to_sheet([colNames_productSummary]);
    ws_pro['!cols'] = [
        15, 15, 15, 15, 15, 15, 15, 15, 15,
        15, 15, 15, 15, 15, 15, 15, 15, 15
    ].map(width => ({ wch: width })); //欄位寬度
    let rowCount_pro = 0;
    appendArray_pro.forEach((array, rowIndex) => {
        rowCount_pro++;
        let cells_pro = [
            array.DATE, array.PRODUCTIVITY, array.SCRAP, array.SCRAP_RATIO, array.A, array.B,
            array.C, array.D, array.E, array.F, array.G, array.H, array.K, array.M, array.N,
            array.Q, array.R, array.T
        ];

        if (2 < rowIndex) {
            cells_pro.forEach((col, colIndex) => {
                if (null === col) {
                    col = '';
                } else if ('string' !== typeof col) {
                    col = parseFloat(col).toFixed(2);
                }
                let value = col.toString().trim().replace(/\n/g, '\r\n');
                let cell = {
                    v: value,
                    t: 's',
                    s: {
                        alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                    },
                    f: value,
                };

                const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
                ws_pro[addr] = cell;
            });
        }
        else {
            cells_pro.forEach((col, colIndex) => {
                if (null === col) {
                    col = '';
                } else if ('string' !== typeof col) {
                    col = parseFloat(col).toFixed(2);
                }
                let value = col.toString().trim().replace(/\n/g, '\r\n');
                let cell = {
                    v: value,
                    t: 's',
                    s: {
                        alignment: { vertical: 'top', horizontal: 'right', wrapText: true },
                    },
                };

                const addr = XLSX.utils.encode_cell({ c: colIndex, r: rowIndex + 1 });
                ws_pro[addr] = cell;
            });
        }
    });

    let lastAddr_pro = XLSX.utils.encode_cell({ c: Math.max(colNames_productSummary.length - 1, 0), r: Math.max(rowCount_pro, 1) });
    ws_pro['!ref'] = `A1:${lastAddr_pro}`;

    XLSX.utils.book_append_sheet(wb_pro, ws_pro, 'report_1');
    XLSX.writeFile(wb_pro, filePath_product, { type: 'file', bookType: 'xlsx' });

    const mailSendKind = 'WEEKLY';
    let mailList = await getMailAddressee(mailSendKind, user);
    // let mailList = '邱亮智 <liangchih_chiu@ccpgp.com>'; //測試用

    const to = mailList;
    const subject = `《${('weekly' === type) ? '週報' : '月報'}》PBTc數位生管系統`;
    const html = htmlString;
    const cc = null;
    const bcc = null;
    const attachments = [filePath, filePath_product];
    return sendMail(to, subject, html, cc, bcc, attachments);
};

//包裝結束的殘包通知
export const alarmOnPackingScheduleFinish = async (user, scheduleData, remainBagInfo) => {
    const mailSendKind = 'PACKING_SCHEDULE_FINISH';
    const mailList = await getMailAddressee(mailSendKind, user);
    if (!mailList) {
        return;
    }

    const to = mailList;
    const subject = `PBTc殘包未使用通知(包裝排程已完成) 日期: ${moment(scheduleData.PACKING_DATE).format('YYYY/MM/DD')} 成品簡碼: ${scheduleData.PRD_PC}`;
    let html = `
        <h1>殘包未使用通知(包裝排程已完成)</h1>
        <b>包裝排程</b>
        <table border="1">
            <thead>
                <tr><th>排程日期</th><th>班別</th><th>包裝機</th><th>成品簡碼</th><th>批號</th><th>包裝狀況</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>${moment(scheduleData.PACKING_DATE).format('YYYY/MM/DD')}</td>
                    <td>${scheduleData.WORK_SHIFT}</td>
                    <td>${scheduleData.PACKING_LINE_NAME}</td>
                    <td>${scheduleData.PRD_PC}</td>
                    <td>${scheduleData.LOT_NO}</td>
                    <td>${scheduleData.PACKING_STATUS}</td>
                </tr>
            <tbody>
        </table>
        <br />
        <b>殘包</b>
        <table border="1">
            <thead>
                <tr><th>規格</th><th>批號</th><th>重量(KG)</th><th>格位</th><th>入庫日期</th><th>存放天數</th></tr>
            </thead>
            <tbody>
                ${remainBagInfo.map(bagInfo => `
                <tr>
                    <td>${bagInfo.PRD_PC}</td>
                    <td>${bagInfo.LOT_NO}</td>
                    <td>${bagInfo.WEIGHT}</td>
                    <td>${bagInfo.LONO}</td>
                    <td>${moment(bagInfo.INV_DATE).format('YYYY-MM-DD HH:mm:ss')}</td>
                    <td>${moment().diff(bagInfo.INV_DATE, 'days')}</td>
                </tr>` ).join()}
            <tbody>
        </table>
        <hr />
        <span style="font-size:8px">此信件由系統自動發出，請勿直接回信</span>        
        `;
    const cc = null;
    const bcc = null;
    return sendMail(to, subject, html, cc, bcc);
};
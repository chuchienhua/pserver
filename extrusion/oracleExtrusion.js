import config from '../config.js';
import { getNowDatetimeString, getOpno, getInvShtNo, getInvInDateSeq, getInvtDate } from '../libs.js';
import oracledb from 'oracledb';
import * as storageDB from './oracleStorage.js';
import * as PrinterAPI from '../printLabel.js';
import * as Mailer from '../mailer.js';
import moment from 'moment';
import axios from 'axios';
import { saveERPPostingRecord } from './oracleStorage.js';

//建立回收料頭太空袋標籤
export async function createScrapBag(printer, bagSeries, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            INSERT INTO PBTC_IOT_EXTR_SCRAP_DETAIL ( BATCH_NO, BAG_SERIES, CREATOR, CREATOR_NAME, CREATE_TIME, COMPANY, FIRM )
            SELECT 
                'S' || TO_CHAR( :CREATE_TIME, 'YYYYMMDD' ) || SUBSTR( TO_CHAR( COUNT(*) + 1, '000') , -3 ),  --流水碼S20230112001
                :BAG_SERIES, :CREATOR, :CREATOR_NAME, :CREATE_TIME, :COMPANY, :FIRM
            FROM PBTC_IOT_EXTR_SCRAP_DETAIL 
            WHERE TO_CHAR( CREATE_TIME, 'YYYYMMDD' ) = TO_CHAR( :CREATE_TIME, 'YYYYMMDD' )
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            BAG_SERIES: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: bagSeries.toString() },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            CREATOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });

        //取得剛剛Insert的BatchNo
        sql = `
            SELECT BATCH_NO
            FROM PBTC_IOT_EXTR_SCRAP_DETAIL
            WHERE BAG_SERIES = :BAG_SERIES
            AND CREATOR = :CREATOR
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY CREATE_TIME DESC `;
        params = {
            BAG_SERIES: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: bagSeries.toString() },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        if (!result.rows.length) {
            throw new Error('建立失敗');
        }

        const apiResult = await PrinterAPI.printScrapAPI(printer, bagSeries, result.rows[0].BATCH_NO, user);
        if (apiResult.error) {
            throw new Error('列印失敗', apiResult.res);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'createScrapBag Error', err);
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

//查詢日期期間內所有已建立的太空袋
export async function getScrapBag(startDate, endDate, bagSeries, line, lotNO, seq, prdPC, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT  
                ES.BATCH_NO, ES.LINE, ES.CREATOR, ES.SEQUENCE, ES.WEIGHT, ES.CREATE_TIME, ES.CREATOR_NAME,
                ES.PRD_PC, ESD.BAG_SERIES, PRO.LOT_NO
            FROM PBTC_IOT_EXTR_SCRAP ES 
                INNER JOIN PBTC_IOT_EXTR_SCRAP_DETAIL ESD
                    ON ES.BATCH_NO = ESD.BATCH_NO
                INNER JOIN PRO_SCHEDULE PRO
                    ON ES.LINE = PRO.LINE
                    AND ES.SEQUENCE = PRO.SEQ
                    AND ES.COMPANY = PRO.COMPANY
                    AND ES.FIRM = PRO.FIRM
            WHERE TO_CHAR( ES.CREATE_TIME, 'YYYYMMDD' ) >= :START_DATE
            AND TO_CHAR( ES.CREATE_TIME, 'YYYYMMDD' ) <= :END_DATE
            AND ES.COMPANY = :COMPANY
            AND ES.FIRM = :FIRM `;
        let params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if (bagSeries !== '*') {
            sql += 'AND ESD.BAG_SERIES = :BAG_SERIES ';
            params['BAG_SERIES'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + bagSeries };
        }
        if (line !== '*') {
            sql += 'AND ES.LINE = :LINE ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
        }
        if (seq !== '*') {
            sql += 'AND ES.SEQUENCE = :SEQUENCE ';
            params['SEQUENCE'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(seq) };
        }
        if (prdPC !== '*') {
            sql += 'AND ES.PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prdPC };
        }
        if (lotNO !== '*') {
            sql += 'AND PRO.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNO };
        }
        sql += 'ORDER BY ES.CREATE_TIME DESC ';  //排序
        const options = { outFormat: oracledb.OBJECT };
        let result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getScrapBag Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得回收料頭太空袋內的內容物
export async function getBagDetail(batchNo, user) {
    let obj = {
        res: [],
        creatorName: '',
        bagSeries: '',
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT CREATOR_NAME, BAG_SERIES
            FROM PBTC_IOT_EXTR_SCRAP_DETAIL
            WHERE BATCH_NO = :BATCH_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        result = await conn.execute(sql, params, options);
        if (!result.rows.length) {
            throw new Error('未找到相符的太空袋');
        }
        obj.creatorName = result.rows[0].CREATOR_NAME;
        obj.bagSeries = result.rows[0].BAG_SERIES;

        sql = `
            SELECT 
                BATCH_NO, LINE, SEQUENCE, PRD_PC, WEIGHT, WEIGHT_RESTART, WEIGHT_BREAK, WEIGHT_ABNORMAL, WEIGHING_WEIGHT, LAST_WORK_SHIFT,
                CREATOR, CREATOR_NAME, CREATE_TIME, EDITOR, EDITOR_NAME, EDIT_TIME
            FROM PBTC_IOT_EXTR_SCRAP
            WHERE BATCH_NO = :BATCH_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY CREATE_TIME `;
        result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getBagDetail Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新回收料頭太空袋內容物
export async function updateBags(batchNo, scrapList, type, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date(); //建立時間統一
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (let scrap of scrapList) {
            let totalWeight = scrap.WEIGHT_RESTART + scrap.WEIGHT_BREAK + scrap.WEIGHT_ABNORMAL;

            if ('create' === type) {
                sql = `
                    INSERT INTO PBTC_IOT_EXTR_SCRAP(
                        BATCH_NO, LINE, SEQUENCE, PRD_PC, WEIGHING_WEIGHT, WEIGHT, WEIGHT_RESTART, WEIGHT_BREAK, WEIGHT_ABNORMAL,
                        CREATOR, CREATOR_NAME, CREATE_TIME, COMPANY, FIRM )
                    SELECT 
                        :BATCH_NO, :LINE, :SEQUENCE, PRD_PC, :WEIGHING_WEIGHT, :WEIGHT, :WEIGHT_RESTART, :WEIGHT_BREAK, :WEIGHT_ABNORMAL,
                        :CREATOR, :CREATOR_NAME, :CREATE_TIME, :COMPANY, :FIRM
                    FROM PRO_SCHEDULE
                    WHERE LINE = :LINE
                    AND SEQ = :SEQUENCE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM `;
                params = {
                    BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchNo.toString() },
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scrap.LINE },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.SEQUENCE) },
                    WEIGHING_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHING_WEIGHT || 0) },
                    WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: totalWeight },
                    WEIGHT_RESTART: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_RESTART || 0) },
                    WEIGHT_BREAK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_BREAK || 0) },
                    WEIGHT_ABNORMAL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_ABNORMAL || 0) },
                    CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                    CREATOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                    CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                const result = await conn.execute(sql, params, { autoCommit: false });
                if (!result.rowsAffected) {
                    throw new Error('料頭輸入異常', scrap);
                }

                //取出該料頭的成品簡碼、排程時間與批號
                sql = ' SELECT PRD_PC, LOT_NO, ACT_STR_TIME, ACT_END_TIME FROM PRO_SCHEDULE WHERE LINE = :LINE AND SEQ = :SEQ AND COMPANY = :COMPANY AND FIRM = :FIRM ';
                params = {
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scrap.LINE },
                    SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.SEQUENCE) },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                if (scheduleResult.rows.length) {
                    await scrapPickAndPay(scheduleResult.rows[0].PRD_PC, totalWeight, scheduleResult.rows[0].LOT_NO, user);

                    //如果超過押出機時間要寄信通知
                    if ((scheduleResult.rows[0].ACT_STR_TIME && scheduleResult.rows[0].ACT_END_TIME) || (!scheduleResult.rows[0].ACT_STR_TIME && !scheduleResult.rows[0].ACT_END_TIME)) {
                        await Mailer.reworkAlarm(scrap.LINE, scrap.SEQUENCE, totalWeight, user);
                    }
                } else {
                    throw new Error('未找到該工令');
                }

            } else if ('update' === type) {
                //考量到料頭冷卻時間問題，若(領班)有勾選上一班產出的，則抓交接紀錄的上一班主控人員，會有跨日的問題(早班處理夜班)
                let controllerName = scrap.CREATOR_NAME;
                if (scrap.LAST_WORK_SHIFT) {
                    sql = `
                        SELECT * FROM (
                            SELECT IC_NAME 
                            FROM PBTC_IOT_HANDOVER_FORM 
                            WHERE LINE = :LINE 
                            AND COMPANY = :COMPANY 
                            AND FIRM = :FIRM
                            AND WORK_SHIFT = :WORK_SHIFT
                            AND CREATE_TIME < :CREATE_TIME
                            ORDER BY RECORD_DATE DESC )
                        WHERE ROWNUM = 1 `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scrap.LINE },
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + scrap.LAST_WORK_SHIFT },
                        CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(scrap.CREATE_TIME) },
                    };
                    const controllerResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                    if (!controllerResult.rows.length) {
                        throw new Error('該班別交接紀錄尚未建立，無法抓取主控');
                    }
                    controllerName = controllerResult.rows[0].IC_NAME;
                }

                sql = `
                    UPDATE PBTC_IOT_EXTR_SCRAP
                    SET WEIGHING_WEIGHT = :WEIGHING_WEIGHT,
                        WEIGHT = :WEIGHT,
                        WEIGHT_RESTART = :WEIGHT_RESTART,
                        WEIGHT_BREAK = :WEIGHT_BREAK, 
                        WEIGHT_ABNORMAL = :WEIGHT_ABNORMAL,
                        CREATOR_NAME = :CREATOR_NAME,
                        LAST_WORK_SHIFT = :LAST_WORK_SHIFT,
                        EDITOR = :EDITOR,
                        EDITOR_NAME = :EDITOR_NAME,
                        EDIT_TIME = SYSDATE
                    WHERE BATCH_NO = :BATCH_NO
                    AND LINE = :LINE
                    AND SEQUENCE = :SEQUENCE
                    AND CREATE_TIME = :CREATE_TIME
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM `;
                params = {
                    BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchNo.toString() },
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scrap.LINE },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.SEQUENCE) },
                    WEIGHING_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHING_WEIGHT || 0) },
                    WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: totalWeight },
                    WEIGHT_RESTART: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_RESTART || 0) },
                    WEIGHT_BREAK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_BREAK || 0) },
                    WEIGHT_ABNORMAL: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(scrap.WEIGHT_ABNORMAL || 0) },
                    CREATOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: controllerName },
                    LAST_WORK_SHIFT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: scrap.LAST_WORK_SHIFT },
                    EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                    EDITOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                    CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(scrap.CREATE_TIME) },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                await conn.execute(sql, params, { autoCommit: false });

            } else {
                throw new Error('程式錯誤');
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateBags Error', err);
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

//移除空的回收料頭太空袋
export async function removeBags(batchNo, user) {
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

        sql = `
            SELECT SUM(WEIGHT) AS TOTAL_WEIGHT
            FROM PBTC_IOT_EXTR_SCRAP
            WHERE BATCH_NO = :BATCH_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchNo.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.length) {
            if (0 !== result.rows[0].TOTAL_WEIGHT) {
                throw new Error('此料頭太空袋有回收料頭在內');
            }
        }

        sql = `
            DELETE PBTC_IOT_EXTR_SCRAP_DETAIL
            WHERE BATCH_NO = :BATCH_NO
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeBags Error', err);
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

//列印粉碎料頭成品標籤
export async function printCrushScrap(printer, bagSeries, weight, user) {
    let obj = {
        res: '',
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        //先取得自動倉棧板編號，多個批號對一個棧板編號
        let opnoResult = await getOpno(user);
        if (opnoResult.error) {
            throw new Error('產生標籤棧板編號失敗: ' + opnoResult.error);
        }
        const opno = opnoResult.res;

        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //紀錄到TABLE，入料時要抓重量
        sql = `
            INSERT INTO PBTC_IOT_EXTR_SCRAP_CRUSH ( BATCH_NO, BAG_SERIES, OPNO, WEIGHT, CREATOR, CREATE_TIME, COMPANY, FIRM )
            SELECT 
                'C' || TO_CHAR( :CREATE_TIME, 'YYYYMMDD' ) || SUBSTR( TO_CHAR( COUNT(*) + 1, '000') , -3 ),  --流水碼C20230112001
                :BAG_SERIES, :OPNO, :WEIGHT, :CREATOR, :CREATE_TIME, :COMPANY, :FIRM
            FROM PBTC_IOT_EXTR_SCRAP_CRUSH 
            WHERE TO_CHAR( CREATE_TIME, 'YYYYMMDD' ) = TO_CHAR( :CREATE_TIME, 'YYYYMMDD' )
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM`;
        params = {
            BAG_SERIES: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: bagSeries.toString() },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: opno },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });

        //取得剛剛Insert的BatchNo
        sql = `
            SELECT BATCH_NO
            FROM PBTC_IOT_EXTR_SCRAP_CRUSH
            WHERE OPNO = :OPNO 
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: opno },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        if (!result.rows.length) {
            throw new Error('建立失敗');
        }
        const batchNo = result.rows[0].BATCH_NO;

        const apiResult = await PrinterAPI.printProductAPI(printer, opno, batchNo, 'OFFPBT', 1, weight, weight, 'T10', '太空袋', user);
        if (apiResult.error) {
            throw new Error('列印失敗', apiResult.res);
        }

        //粉碎料頭是取"料頭太空袋"內的來粉碎，不需再做繳庫
        /*
        let payResult = await crushScrapPay(weight, opno, user);
        if (payResult.error) {
            throw new Error('過帳繳庫失敗');
        }
        */
    } catch (err) {
        console.error(getNowDatetimeString(), 'printCrushScrap Error', err);
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

//列印前料標籤並記錄
export async function createHeadMaterial(printer, line, sequence, productNo, lotNo, weight, prdReason, remark, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        //先取得自動倉棧板編號，一個批號對一個棧板編號
        let opnoResult = await getOpno(user);
        if (opnoResult.error) {
            throw new Error('產生標籤棧板編號失敗: ' + opnoResult.error);
        }
        const opno = opnoResult.res;

        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_EXTR_HEAD(BATCH_NO, LINE, SEQUENCE, PRD_PC, WEIGHT, CREATOR, CREATOR_NAME, CREATE_TIME, OPNO, COMPANY, FIRM, PRD_REASON, REMARK)
            SELECT
            'H' || TO_CHAR( : CREATE_TIME, 'YYYYMMDD') || SUBSTR(TO_CHAR(COUNT(*) + 1, '000'), -3), --流水碼H20230112001
                : LINE, : SEQUENCE, : PRD_PC, : WEIGHT, : CREATOR, : CREATOR_NAME, : CREATE_TIME, :OPNO, : COMPANY, : FIRM, :PRD_REASON, :REMARK
            FROM PBTC_IOT_EXTR_HEAD 
            WHERE TO_CHAR(CREATE_TIME, 'YYYYMMDD') = TO_CHAR( : CREATE_TIME, 'YYYYMMDD')
            AND COMPANY = : COMPANY
            AND FIRM = : FIRM`;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            CREATOR_NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: opno },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PRD_REASON: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prdReason },
            REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + remark }
        };
        const result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            throw new Error('前料輸入異常', line, sequence, productNo, lotNo, weight);
        }

        const printProductNo = 'OFFPBT01' + productNo;
        const apiResult = await PrinterAPI.printProductAPI(printer, opno, lotNo, printProductNo, 1, weight, weight, 'P40', '規格紙袋', user);
        if (apiResult.error) {
            throw new Error('列印失敗', apiResult.res);
        }

        let payResult = await headPickAndPay(productNo, weight, lotNo, opno, user);
        if (payResult.error) {
            throw new Error('過帳繳庫失敗');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'createHeadMaterial Error', err);
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

//查詢所有前料產出紀錄
export async function getHeadMaterial(startDate, endDate, lineSearch, seqSearch, productNoSearch, lotNoSearch, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT S0.LINE, S0.SEQUENCE, S0.PRD_PC, S0.WEIGHT, S0.CREATOR, S0.CREATOR_NAME, S0.CREATE_TIME, S0.PRD_REASON, S0.REMARK, S1.LOT_NO
            FROM PBTC_IOT_EXTR_HEAD S0 LEFT JOIN PRO_SCHEDULE S1
                ON S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
                AND S0.LINE = S1.LINE
                AND S0.SEQUENCE = S1.SEQ
            WHERE TO_CHAR(S0.CREATE_TIME, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(S0.CREATE_TIME, 'YYYYMMDD') <= :END_DATE
            AND S0.COMPANY = : COMPANY
            AND S0.FIRM = : FIRM `;
        let params = {
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: startDate.toString() },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: endDate.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('*' !== lineSearch) {
            sql += ' AND S0.LINE = :LINE ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lineSearch };
        }
        if ('*' !== seqSearch) {
            sql += ' AND S0.SEQUENCE = :SEQUENCE ';
            params['SEQUENCE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + seqSearch };
        }
        if ('*' !== productNoSearch) {
            sql += ' AND S0.PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNoSearch };
        }
        if ('*' !== lotNoSearch) {
            sql += ' AND S1.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNoSearch };
        }

        sql += ' ORDER BY CREATE_TIME ';
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getHeadMaterial Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//前料領料(改番)後繳庫程式
export async function headPickAndPay(productNo, weight, lotNo, opno, user) {
    let obj = {
        res: '',
        error: false,
    };

    const apiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    const SHEET_ID = 'PT5';
    try {
        //取得出入庫帳日期
        const date = getInvtDate(new Date());

        //產生領料過帳單號
        let shtNoResult = await getInvShtNo(SHEET_ID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        let shtNo = shtNoResult.res;

        //找被扣儲位的入庫日期序號
        const invResult = await storageDB.getLotNoInvtDate(user, lotNo, productNo);
        if (!invResult.inDateSeq.length) {
            const InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
            if (InDateSeqResult.error) {
                throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
            }
            invResult.inDateSeq = InDateSeqResult.res;
        }

        //領料(改番)，由已繳庫的成品轉為OFFPBT||OFFPBT01成品
        let bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': SHEET_ID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': 'OFFPBT01', //成品
            'PM': 'P', //(P成品/M原料)
            'PCK_KIND': '0',
            'PCK_NO': '*',
            'MAT_PC': productNo, //原成品
            'QTY': weight,
            'LOT_NO': lotNo, //主排程LOT_NO
            'CCPCODE': 'B103',
            'SIGN': '-', //領料固定為負號
            'QSTATUS': 'C',
            'INDATESEQ': invResult.inDateSeq, //需要扣儲位帳的INDATESEQ
            'LOC': invResult.loc, //被扣帳的儲位
            'CREATOR': '' + user.PPS_CODE
        }];
        let apiResult = await axios.post(apiURL, bodyData, { proxy: false });
        console.log(`OFFPBT01; ${lotNo}; 領料改番${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = weight;
        await saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'OFFPBT01');

        //產生繳庫過帳單號
        shtNoResult = await getInvShtNo(SHEET_ID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        shtNo = shtNoResult.res;

        //繳庫
        bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': SHEET_ID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': 'OFFPBT01', //成品
            'PCK_KIND': '0', //包材重
            'PCK_NO': '*', //包材種類
            'QTY': weight, //繳庫重量
            'LOT_NO': lotNo, //主排程LOT_NO
            'CCPCODE': 'E171',
            'SIGN': '+', //繳庫正負號
            'QSTATUS': '3',
            'CREATOR': '' + user.PPS_CODE, //會再調整為自動
            'INDATESEQ': opno, //先用opno，原為getInvInDateSeq產生
            'LOC': '7PLD2007' //固定
        }];
        apiResult = await axios.post(apiURL, bodyData, { proxy: false });
        console.log(`OFFPBT01; ${lotNo}; 前料繳庫${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = weight;
        await saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'OFFPBT01');
    } catch (err) {
        console.error(getNowDatetimeString(), 'headPickAndPay', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//紀錄太空袋產出料頭時做領料(改番)再做繳庫
export async function scrapPickAndPay(productNo, weight, lotNo, user) {
    let obj = {
        res: '',
        error: false,
    };

    const apiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    const SHEET_ID = 'PT5';
    try {
        //取得出入庫帳日期
        const date = getInvtDate(new Date());

        //產生領料改番過帳單號
        let shtNoResult = await getInvShtNo(SHEET_ID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        let shtNo = shtNoResult.res;

        //找被扣儲位的入庫日期序號
        const invResult = await storageDB.getLotNoInvtDate(user, lotNo, productNo);
        if (!invResult.inDateSeq.length) {
            const InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
            if (InDateSeqResult.error) {
                throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
            }
            invResult.inDateSeq = InDateSeqResult.res;
        }

        //領料(改番)，由已繳庫的成品轉為OFFPBT成品
        let bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': SHEET_ID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': 'OFFPBT', //成品
            'PM': 'P', //(P成品/M原料)
            'PCK_KIND': '0',
            'PCK_NO': '*',
            'MAT_PC': productNo, //原成品
            'QTY': weight,
            'LOT_NO': lotNo, //主排程LOT_NO
            'CCPCODE': 'B103',
            'SIGN': '-', //領料固定為負號
            'QSTATUS': 'C',
            'INDATESEQ': invResult.inDateSeq, //需要扣儲位帳的INDATESEQ
            'LOC': invResult.loc, //被扣帳的儲位
            'CREATOR': '' + user.PPS_CODE
        }];
        let apiResult = await axios.post(apiURL, bodyData, { proxy: false });
        console.log(`OFFPBT; ${lotNo}; 領料改番${apiResult.data[0][2]}; ${bodyData}`);

        bodyData[0].WEIGHT = weight;
        await saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'OFFPBT');

        //產生繳庫過帳單號
        shtNoResult = await getInvShtNo(SHEET_ID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        shtNo = shtNoResult.res;

        //產生儲位繳庫入庫日期序號
        const InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
        if (InDateSeqResult.error) {
            throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
        }
        const inDateSeq = InDateSeqResult.res;

        //繳庫
        bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': SHEET_ID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': 'OFFPBT', //成品
            'PCK_KIND': '0', //包材重
            'PCK_NO': '*', //包材種類
            'QTY': weight, //繳庫重量
            'LOT_NO': lotNo, //主排程LOT_NO
            'CCPCODE': 'E171',
            'SIGN': '+', //繳庫正負號
            'QSTATUS': '3',
            'CREATOR': '' + user.PPS_CODE, //會再調整為自動
            'INDATESEQ': inDateSeq, //getInvInDateSeq產生
            'LOC': '7PLD2007' //固定
        }];
        apiResult = await axios.post(apiURL, bodyData, { proxy: false });
        console.log(`OFFPBT; ${lotNo}; 料頭繳庫${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = weight;
        await saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'OFFPBT');

    } catch (err) {
        console.error(getNowDatetimeString(), 'reworkPay', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}

//粉碎料頭繳庫程式
export async function crushScrapPay(weight, opno, user) {
    let obj = {
        res: '',
        error: false,
    };

    const apiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    const SHEET_ID = 'PT5';
    try {
        //取得出入庫帳日期
        const date = getInvtDate(new Date());

        //產生過帳單號
        let shtNoResult = await getInvShtNo(SHEET_ID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        const shtNo = shtNoResult.res;

        //產生儲位入庫日期序號
        /*
        let InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
        if (InDateSeqResult.error) {
            throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
        }
        const inDateSeq = InDateSeqResult.res;
        */

        //繳庫
        let bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': SHEET_ID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': 'OFFPBT', //成品
            'PCK_KIND': '0', //包材重
            'PCK_NO': '*', //包材種類
            'QTY': weight, //繳庫重量
            'LOT_NO': opno, //無法得知批號，使用棧板編號
            'CCPCODE': 'E100',
            'SIGN': '+', //繳庫正負號
            'QSTATUS': '3',
            'CREATOR': '' + user.PPS_CODE,
            'INDATESEQ': opno, //先用opno，原為getInvInDateSeq產生
            'LOC': '7PLD2007' //固定
        }];
        const apiResult = await axios.post(apiURL, bodyData, { proxy: false });
        console.log(`OFFPBT; ${opno}; 粉碎料頭繳庫${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = weight;
        await saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'OFFPBT');
    } catch (err) {
        console.error(getNowDatetimeString(), 'crushScrapPay', err);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
}
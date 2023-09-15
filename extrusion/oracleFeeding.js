import config from '../config.js';
import { getNowDatetimeString, getInvShtNo, getInvInDateSeq, getInvtDate } from '../libs.js';
import oracledb from 'oracledb';
import * as storageDB from './oracleStorage.js';
import * as VisionTagsAPI from '../VisionTagsAPI.js';
import * as Mailer from '../mailer.js';
import * as remainBagDB from '../packing/oraclePackingRemain.js';
import moment from 'moment';
import axios from 'axios';

//押出入料部分
//取得每一條線正在處理或下一筆的工令
export async function getWorkingOrders(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT LINE, SEQ
            FROM PRO_SCHEDULE
            WHERE ACT_STR_TIME IS NOT NULL AND ACT_END_TIME IS NULL
            AND ACT_STR_TIME > TO_DATE('20221001', 'YYYYMMDD')
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY LINE, SEQ DESC `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        let result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getWorkingOrders Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得押出入料區管制表
export async function getFeedingForm(line, sequence, user) {
    let obj = {
        res: [],
        exist: false,
        startTime: '',
        endTime: '',
        siloName: '',
        siloMaterial: '',
        feederWeight: 0,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查押出管制表是否已存在
        sql = `
            SELECT 
                S0.FEEDER_NO, S0.REWORK_SOURCE, S0.MATERIAL, S0.SEMI_NO, S0.FEED_NUM, S0.FEED_WEIGHT, S0.NEED_WEIGHT, 
                S0.SEMI_NO, S0.FOREIGN, S0.AUDIT_STATUS, S0.CREATE_TIME, S0.CREATOR,
                S1.ACT_STR_TIME, S1.ACT_END_TIME
            FROM PBTC_IOT_EXTRUSION S0 
                LEFT JOIN PRO_SCHEDULE S1
                ON S0.LINE = S1.LINE
                AND S0.SEQUENCE = S1.SEQ
                AND S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
            WHERE S0.LINE = :LINE
            AND S0.SEQUENCE = :SEQUENCE
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            ORDER BY S0.FEEDER_NO `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: sequence.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        let result = await conn.execute(sql, params, options);
        if (result.rows.length) {
            obj.res = result.rows;
            obj.exist = true;
            obj.startTime = result.rows[0].ACT_STR_TIME;
            obj.endTime = result.rows[0].ACT_END_TIME;
        }

        //取得線別下目前所使用的SILO與M1入料量，回傳{ SILO: SILO號(5), M1: M1累計量 }
        let siloTags = await VisionTagsAPI.getLineUsingSilo(line, user);
        if (!siloTags.data.error) {
            siloTags = Object.values(siloTags.data.tags);
            obj.siloName = '7MP2S00' + siloTags[0]; //應有C01~C06，儲位改為7MP2S001~7MP2S006
            obj.feederWeight = siloTags[1];
        }

        //檢查SILO與當前MI入料機的樹酯原料是否相同
        sql = `
            SELECT PRD_PC
            FROM LOCINV_D
            WHERE LOC = :LOC
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY IN_DATE DESC `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: obj.siloName },
        };
        result = await conn.execute(sql, params, options);
        if (result.rows.length) {
            obj.siloMaterial = result.rows[0].PRD_PC;
        } else {
            obj.siloMaterial = `SILO儲位${obj.siloName}尚未建立`;
        }

        //若未建立過，回傳配方料，確認後再建立
        if (!obj.exist) {
            sql = `
                SELECT
                    PBTC_IOT_RECIPE.SEMI_NO,
                    SUM(PBTC_IOT_RECIPE.RATIO) AS RATIO, 
                    PBTC_IOT_RECIPE.FEEDER AS FEEDER_NO,
                    ( SUM(PRO_SCHEDULE.PRO_WT) * SUM(PBTC_IOT_RECIPE.RATIO) * 0.01 ) AS NEED_WEIGHT,
                    ( CASE 
                        WHEN PBTC_IOT_RECIPE.SEMI_NO = 'M' 
                        THEN LISTAGG( PBTC_IOT_RECIPE.MATERIAL, ',' ) WITHIN GROUP ( ORDER BY PBTC_IOT_RECIPE.MATERIAL )
                        ELSE PBTC_IOT_RECIPE.SEMI_NO || PRO_SCHEDULE.PRD_PC 
                    END ) AS MATERIAL,
                    PRO_SCHEDULE.ACT_STR_TIME AS START_TIME,
                    PRO_SCHEDULE.ACT_END_TIME AS END_TIME
                FROM PRO_SCHEDULE, PBTC_IOT_RECIPE
                WHERE PRO_SCHEDULE.LINE = :LINE
                AND PRO_SCHEDULE.SEQ = :SEQUENCE
                AND PRO_SCHEDULE.COMPANY = :COMPANY
                AND PRO_SCHEDULE.FIRM = :FIRM
                AND PRO_SCHEDULE.PRD_PC = PBTC_IOT_RECIPE.PRODUCT_NO
                AND PBTC_IOT_RECIPE.CREATE_TIME = ( 
                    SELECT MAX( CREATE_TIME ) 
                    FROM PBTC_IOT_RECIPE
                    WHERE PRODUCT_NO = PRO_SCHEDULE.PRD_PC
                    AND VER = PRO_SCHEDULE.SCH_SEQ
                    AND LINE = :LINE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM )
                GROUP BY PBTC_IOT_RECIPE.SEMI_NO, PBTC_IOT_RECIPE.FEEDER, PRO_SCHEDULE.PRD_PC, PRO_SCHEDULE.ACT_STR_TIME, PRO_SCHEDULE.ACT_END_TIME
                ORDER BY PBTC_IOT_RECIPE.FEEDER`;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, options);

            if (result.rows.length) {
                obj.res = result.rows;
                obj.startTime = result.rows[0].START_TIME;
                obj.endTime = result.rows[0].END_TIME;
            } else {
                obj.res = '未找到與工令相符合之配方';
                obj.error = true;
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFeedingForm Error', err);
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

//建立押出入料管制表
export async function createFeedingForm(line, sequence, materialArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date(); //統一紀錄建立日期
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得線別下目前所使用的SILO是幾號，res = { SILO: SILO號(5), M1: M1累計量 }
        let siloLotNo = 'NOT FOUND';
        let siloProductNo = 'NOT FOUND';
        let siloName = 'NOT FOUND'; //漳州廠目前沒有Tags
        let siloTags = await VisionTagsAPI.getLineUsingSilo(line, user);
        if (!siloTags.data.error) {
            //取得SILO使用的樹酯批號
            siloName = '7MP2S00' + Object.values(siloTags.data.tags)[0]; //應有C01~C06，儲位改為7MP2S001~7MP2S006
            sql = `
                SELECT LOT_NO, PRD_PC
                FROM LOCINV_D
                WHERE LOC = :LOC
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                ORDER BY IN_DATE DESC `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: siloName },
            };
            let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                siloLotNo = result.rows[0].LOT_NO;
                siloProductNo = result.rows[0].PRD_PC;
            }
        }

        for (const material of materialArray) {
            sql = `
                INSERT INTO PBTC_IOT_EXTRUSION ( LINE, SEQUENCE, FEEDER_NO, MATERIAL, SEMI_NO, NEED_WEIGHT, COMPANY, FIRM, CREATE_TIME, CREATOR
                    ${('1' === material.FEEDER_NO.slice(-1)) ? ', SILO_LOT_NO' : ''}
                    ${('1' === material.FEEDER_NO.slice(-1)) ? ', SILO_NO' : ''}
                    ${('1' === material.FEEDER_NO.slice(-1)) ? ', SILO_PRD_PC' : ''} )
                VALUES ( :LINE, :SEQUENCE, :FEEDER_NO, :MATERIAL, :SEMI_NO, :NEED_WEIGHT, :COMPANY, :FIRM, :CREATE_TIME, :CREATOR
                    ${('1' === material.FEEDER_NO.slice(-1)) ? `, '${siloLotNo}'` : ''}
                    ${('1' === material.FEEDER_NO.slice(-1)) ? `, '${siloName}'` : ''}
                    ${('1' === material.FEEDER_NO.slice(-1)) ? `, '${siloProductNo}'` : ''} ) `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.FEEDER_NO.toString() },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.SEMI_NO.toString() },
                NEED_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(material.NEED_WEIGHT) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            };
            const commit = { autoCommit: false };
            await conn.execute(sql, params, commit);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'createFeedingForm Error', err);
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

//更先押出入料/查核表
export async function updateFeedingForm(line, sequence, materialArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        for (const material of materialArray) {
            sql = `
                UPDATE PBTC_IOT_EXTRUSION
                SET AUDIT_STATUS = :AUDIT_STATUS,
                    FOREIGN = :FOREIGN
                WHERE LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND MATERIAL = :MATERIAL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                AUDIT_STATUS: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: (material.AUDIT) ? 1 : 0 },
                FOREIGN: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: (material.FOREIGN) ? 1 : 0 },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };

            const commit = { autoCommit: false };
            await conn.execute(sql, params, commit);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateFeedingForm Error', err);
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

//刪除未入料過的管制表
export async function removeFeedingForm(line, sequence, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查是否有入料過
        sql = `
            SELECT SUM(FEED_NUM) AS FEED_NUM
            FROM PBTC_IOT_EXTRUSION 
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            GROUP BY LINE, SEQUENCE `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        //刪除未入料過的管制表
        if (result.rows.length) {
            if (0 === result.rows[0].FEED_NUM) {
                sql = `
                    DELETE PBTC_IOT_EXTRUSION
                    WHERE LINE = :LINE
                    AND SEQUENCE = :SEQUENCE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM `;
                params = {
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
                await conn.execute(sql, params, { autoCommit: true });

            } else {
                throw new Error('僅允許刪除未入料過的排程');
            }

        } else {
            throw new Error('未找到該入料管制表');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeFeedingForm Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//押出入料扣帳
export async function powderfeeding(line, sequence, material, feedNum, feedWeight, feedLotNo, feedBatchNo, semiNo, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    let date = new Date(); //統一紀錄領料日期
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //若不是半成品，則檢查品檢結果
        let qaResult = '';
        if ('P' !== semiNo && 'G' !== semiNo) {
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
                        LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feedLotNo },
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
                            let sendMailsuccess = await Mailer.pickingAlarm(feedLotNo, feedBatchNo, line, sequence, material, qaResult, 'extrusion', user);
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

            //儲位扣帳，FIXME:李課長儲位DB欄位持續調整中，所以手動建立的標籤有儲位嗎？
            sql = `
                UPDATE LOCINV_D${('7' !== user.FIRM) ? '@ERPTEST' : ''}
                SET QTY = QTY - ( PCK_KIND * :PICK_NUM ),
                    PLQTY = PLQTY - :PICK_NUM
                WHERE PAL_NO = :BATCH_NO
                AND PLQTY >= :PICK_NUM --收料作業早期的原料棧板'棧板數量'異常為0
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedNum) },
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feedBatchNo.toString() }, //領料棧板編號
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { autoCommit: false });
            if (!result.rowsAffected) {
                throw new Error('領用包數不可超過儲位棧板包數');
            }
        }

        //更新押出入料量
        sql = `
            UPDATE PBTC_IOT_EXTRUSION
            SET FEED_NUM = FEED_NUM + :FEED_NUM,
                FEED_WEIGHT = FEED_WEIGHT + :FEED_WEIGHT
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND MATERIAL = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            FEED_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedNum) },
            FEED_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedWeight) },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });
        if (!result.rowsAffected) {
            throw new Error('未正常勾稽');
        }

        //記錄備料入料紀錄
        sql = `
            INSERT INTO PBTC_IOT_PICKING_RECORD ( LINE, SEQUENCE, LOT_NO, BATCH_NO, SEMI_NO, MATERIAL, WEIGHT, PICK_DATE, PICK_NUM, QA_RESULT, PPS_CODE, NAME, COMPANY, FIRM, STAGE )
            VALUES ( :LINE, :SEQUENCE, :LOT_NO, :BATCH_NO, :SEMI_NO, :MATERIAL, :WEIGHT, :PICK_DATE, :PICK_NUM, :QA_RESULT, :PPS_CODE, :NAME, :COMPANY, :FIRM, 'EXTRUSION' ) `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feedLotNo ? feedLotNo.toString() : '' }, //半成品無原料批號
            BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feedBatchNo.toString() },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: semiNo.toString() },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: feedWeight },
            PICK_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
            PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedNum) },
            QA_RESULT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: qaResult },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'powderfeeding Error', err);
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

//取得重工品料頭、前料、包裝棧板的資訊
export async function getReworkData(reworkSource, opno, user) {
    let obj = {
        pickNum: 0,
        totalNum: 0,
        error: false,
    };

    let conn;
    let sql;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        if ('scrap' === reworkSource) {
            sql = `
                SELECT S0.WEIGHT, S1.PICK_NUM
                FROM PBTC_IOT_EXTR_SCRAP_CRUSH S0 LEFT JOIN PBTC_IOT_PICKING_RECORD S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.OPNO = S1.BATCH_NO
                WHERE S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                AND S0.OPNO = :OPNO `;

        } else if ('head' === reworkSource) {
            sql = `
                SELECT S0.WEIGHT, S1.PICK_NUM
                FROM PBTC_IOT_EXTR_HEAD S0 LEFT JOIN PBTC_IOT_PICKING_RECORD S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.OPNO = S1.BATCH_NO
                WHERE S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM
                AND S0.OPNO = :OPNO `;

        } else if ('return' === reworkSource) {
            sql = `
                SELECT 
                    SUM(S0.DETAIL_SEQ_END - S0.DETAIL_SEQ_START - S0.SEQ_ERROR_COUNT + 1) / COUNT(*) AS TOTAL_NUM,
                    SUM(S1.PICK_NUM) AS PICK_NUM
                FROM PBTC_IOT_PACKING_DETAIL S0 LEFT JOIN PBTC_IOT_PICKING_RECORD S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.PALLET_NO = S1.BATCH_NO
                WHERE S0.PALLET_NO = :OPNO
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM `;

        } else {
            throw new Error('重工品選擇異常');
        }

        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            obj.pickNum = result.rows[0].PICK_NUM || 0;
            obj.totalNum = result.rows[0].TOTAL_NUM || 1; //料頭、前料固定為1

        } else {
            throw new Error('未找到相符的標籤資訊');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getReworkData Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//押出入料重工品
export async function reworkFeeding(line, sequence, reworkSource, feederNo, reworkPickNum, opno, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    const pickApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟Procedure的API
    const payApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_pbtc'; //游晟繳庫Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得出入庫帳日期
        const date = getInvtDate(new Date());

        let materialName = ''; //重工品原料名稱
        let bagType = ''; //包裝性質
        let reworkLotNo = ''; //重工品(原產品的)批號
        let reworkloc = ''; //重工品儲位
        switch (reworkSource) {
            case 'scrap':
                materialName = 'OFFPBT';
                bagType = 'T10';
                reworkloc = '7PLD2007';
                break;
            case 'head':
                materialName = 'OFFPBT01';
                bagType = 'P40';
                reworkloc = '7PLD2007';
                break;
            case 'return':
                //materialName = 'RETURN'; //還不確定，從包裝排程抓
                //bagType = ''; //從包裝排程取
                break;
            default:
                break;
        }

        //檢查重工品OPNO棧板編號是否存在，並取得棧板重量
        let feedWeight = 0;
        if ('head' === reworkSource || 'scrap' === reworkSource) {
            const tableName = ('head' === reworkSource) ? 'PBTC_IOT_EXTR_HEAD' : 'PBTC_IOT_EXTR_SCRAP_CRUSH';
            sql = `
                SELECT S0.WEIGHT, S1.LOT_NO
                FROM ${tableName} S0 JOIN PRO_SCHEDULE S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.LINE = S1.LINE
                    AND S0.SEQUENCE = S1.SEQ
                WHERE S0.OPNO = :OPNO
                AND S0.COMPANY = :COMPANY
                AND S0.FIRM = :FIRM `;
            params = {
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: opno.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (!result.rows.length) {
                throw new Error('未找到此棧板編號');
            }
            feedWeight = result.rows[0].WEIGHT;
            reworkLotNo = result.rows[0].LOT_NO;

        } else if ('return' === reworkSource) {
            //從包裝排程抓回爐品產品簡碼，並檢查是否存在或處於其他狀態
            sql = `
                SELECT SCH.PRD_PC, SCH.PACKING_WEIGHT_SPEC, SCH.PACKING_MATERIAL_ID, SCH.LOT_NO, SCH.SILO_NO
                FROM PBTC_IOT_PACKING_DETAIL DET, PBTC_IOT_PACKING_SCHEDULE SCH
                WHERE DET.PACKING_SEQ = SCH.PACKING_SEQ
                AND DET.PALLET_NO = :OPNO
                AND DET.COMPANY = :COMPANY
                AND DET.FIRM = :FIRM `;
            params = {
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: opno.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (!result.rows.length) {
                throw new Error('未找到回爐品已包裝棧板編號');
            }
            feedWeight = result.rows[0].PACKING_WEIGHT_SPEC * reworkPickNum;
            bagType = result.rows[0].PACKING_MATERIAL_ID;
            reworkLotNo = result.rows[0].LOT_NO;
            materialName = result.rows[0].PRD_PC;
            reworkloc = result.rows[0].SILO_NO;

        } else {
            throw new Error('重工品選擇異常');
        }

        //檢查料頭/前料/回爐品各別的入料機是否一致，若已入過料頭CM5，則CM5鎖定為料頭&其他入料機不能再下料頭
        sql = `
            SELECT REWORK_SOURCE, FEEDER_NO
            FROM PBTC_IOT_EXTRUSION
            WHERE LINE = :LINE
            AND SEQUENCE = :SEQUENCE
            AND FEEDER_NO = :FEEDER_NO
            AND MATERIAL != 'REWORK'
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feederNo },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            throw new Error('該入料機已設定過其他原料');
        }

        //取得主排程的資訊
        sql = ` 
            SELECT LOT_NO, PRD_PC, REPLACE(SILO, '-', '') AS SILO 
            FROM PRO_SCHEDULE 
            WHERE LINE = :LINE AND SEQ = :SEQ AND COMPANY = :COMPANY AND FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (!result.rows.length) {
            throw new Error('主排程未找到該工令');
        }
        const lotNo = result.rows[0].LOT_NO;
        const productNo = result.rows[0].PRD_PC;
        const loc = result.rows[0].SILO;

        //取得領料(重工)過帳單號
        let sheetID = 'PT5';
        let shtNoResult = await getInvShtNo(sheetID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        let shtNo = shtNoResult.res;

        //領料(重工)，由已繳庫的OFFPBT||OFFPBT01成品轉為成品
        let reworkInDateSeq = opno; //料頭、前料儲位INDATESEQ為opno
        if ('return' === reworkSource) {
            //找被扣儲位的入庫日期序號
            const invResult = await storageDB.getLotNoInvtDate(user, reworkLotNo, materialName, reworkloc);
            if (!invResult.inDateSeq.length) {
                const InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
                if (InDateSeqResult.error) {
                    throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
                }
                invResult.inDateSeq = InDateSeqResult.res;
            }
            reworkInDateSeq = invResult.inDateSeq;
        }

        let bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': sheetID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': productNo, //成品
            'PM': 'P', //(P成品/M原料)
            'PCK_KIND': feedWeight,
            'PCK_NO': bagType,
            'MAT_PC': materialName, //原成品
            'QTY': feedWeight,
            'LOT_NO': lotNo, //主排程LOT_NO
            'CCPCODE': 'B102', //B103改番領料、B102重工領料
            'SIGN': '-', //領料固定為負號
            'QSTATUS': 'C',
            'INDATESEQ': reworkInDateSeq, //需要扣儲位帳的INDATESEQ
            'LOC': reworkloc, //原成品儲位
            'CREATOR': '' + user.PPS_CODE
        }];
        let apiResult = await axios.post(pickApiURL, bodyData, { proxy: false });
        console.log(`${line}-${sequence}; ${lotNo}; 重工入料，領料改番${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = feedWeight;
        await storageDB.saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], feederNo);

        //產生繳庫過帳單號
        sheetID = 'PT2';
        shtNoResult = await getInvShtNo(sheetID, date);
        if (shtNoResult.error) {
            throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
        }
        shtNo = shtNoResult.res;

        //取得該成品批號的INDATESEQ
        let inDateSeq;
        let oldInDateSeq = await storageDB.getLotNoInvtDate(user, lotNo, productNo, loc);
        if (oldInDateSeq.inDateSeq.length) {
            inDateSeq = oldInDateSeq.inDateSeq;
        } else {
            //產生儲位入庫日期序號
            let inDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
            if (inDateSeqResult.error) {
                throw new Error('產生儲位入庫日期序號失敗: ' + inDateSeqResult.error);
            }
            inDateSeq = inDateSeqResult.res;
        }

        //繳庫
        bodyData = [{
            'DEBUG': TEST_MODE, //(true測試/false正式)
            'SHEET_ID': sheetID,  //固定
            'SHTNO': shtNo, //getInvShtNo產生
            'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
            'PRD_PC': productNo, //成品
            'QTY': feedWeight, //繳庫重量
            'OLDQTY': feedWeight, //只要不是負數，要與QTY帶一樣的
            'SIGN': '+', //繳庫正負號
            'LOT_NO': lotNo, //主排程的批號
            'CCPCODE': 'E170', //E100一般繳庫、E171改番繳庫、E170重工繳庫
            'CREATOR': '' + user.PPS_CODE,
            'INDATESEQ': inDateSeq,
            'LOC': loc //主排程的SILO1
        }];
        apiResult = await axios.post(payApiURL, bodyData, { proxy: false });
        console.log(`${line}-${sequence}; ${lotNo}; 重工入料，繳庫${apiResult.data[0][2]}`);

        bodyData[0].WEIGHT = feedWeight;
        await storageDB.saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'FEED');

        //更新工令重工品入料狀態
        sql = `
            BEGIN
                INSERT INTO PBTC_IOT_EXTRUSION ( LINE, SEQUENCE, MATERIAL, FEEDER_NO, FEED_WEIGHT, CREATE_TIME, REWORK_SOURCE, COMPANY, FIRM )
                VALUES ( :LINE, :SEQUENCE, :MATERIAL, :FEEDER_NO, :FEED_WEIGHT, :CREATE_TIME, :REWORK_SOURCE, :COMPANY, :FIRM );
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE PBTC_IOT_EXTRUSION
                    SET FEED_WEIGHT = FEED_WEIGHT + :FEED_WEIGHT,
                        REWORK_SOURCE = :REWORK_SOURCE
                    WHERE LINE = :LINE
                    AND SEQUENCE = :SEQUENCE
                    AND MATERIAL = :MATERIAL
                    AND FEEDER_NO = :FEEDER_NO
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM;
            END; `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'REWORK' }, //重工統一原料簡碼
            FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: feederNo.toString() },
            FEED_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedWeight) },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
            REWORK_SOURCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: reworkSource },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });

        //紀錄領料紀錄
        sql = `
            INSERT INTO PBTC_IOT_PICKING_RECORD ( LINE, SEQUENCE, BATCH_NO, LOT_NO, SEMI_NO, MATERIAL, WEIGHT, PICK_DATE, PICK_NUM, QA_RESULT, OG, PPS_CODE, NAME, COMPANY, FIRM, STAGE )
            VALUES ( :LINE, :SEQUENCE, :BATCH_NO, :LOT_NO, :SEMI_NO, :MATERIAL, :WEIGHT, :PICK_DATE, :PICK_NUM, 'RE', :OG, :PPS_CODE, :NAME, :COMPANY, :FIRM, 'EXTRUSION' ) `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + reworkLotNo },
            SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'M' }, //押出入料統一為M成品
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: materialName },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feedWeight) },
            PICK_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(reworkPickNum) },
            OG: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: reworkSource }, //重工種類
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });
    } catch (err) {
        console.error(getNowDatetimeString(), 'reworkFeeding', err);
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

//殘包入料
export async function remainBagFeeding(line, sequence, opno, pickType, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    const pickApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet'; //游晟Procedure的API
    const payApiURL = 'http://visionservice.ccpgp.com/api/inventory/inv_pbtc'; //游晟繳庫Procedure的API
    const TEST_MODE = true; //仁武false正式區；漳州true測試區
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得出入庫帳日期
        const date = getInvtDate(new Date());

        //取得殘包相關資料
        const remainBagResult = await remainBagDB.getBagData(user, opno);
        if (remainBagResult.res.length) {
            const remainBagLotNo = remainBagResult.res[0].LOT_NO;
            const remainBagProductNo = remainBagResult.res[0].PRD_PC;
            const remainBagWeight = remainBagResult.res[0].WEIGHT;

            //取得主排程資訊
            sql = ` 
                SELECT LOT_NO, PRD_PC, REPLACE(SILO, '-', '') AS SILO 
                FROM PRO_SCHEDULE 
                WHERE LINE = :LINE AND SEQ = :SEQ AND COMPANY = :COMPANY AND FIRM = :FIRM `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            const scheduleResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (!scheduleResult.rows.length) {
                throw new Error('主排程未找到該工令');
            }
            const lotNo = scheduleResult.rows[0].LOT_NO;
            const productNo = scheduleResult.rows[0].PRD_PC;
            const loc = scheduleResult.rows[0].SILO;

            //改番領料必須要同規格檢查
            if (productNo !== remainBagProductNo && 'pick' === pickType) {
                throw new Error(`回參須為同產品，生產中${productNo}，殘包${remainBagProductNo}`);
            }

            //取得領料(改番/重工)過帳單號
            let sheetID = 'PT5';
            let shtNoResult = await getInvShtNo(sheetID, date);
            if (shtNoResult.error) {
                throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
            }
            let shtNo = shtNoResult.res;

            //領料(改番/重工)，由已繳庫的殘包成品轉為成品
            const remainBagInvtResult = await storageDB.getLotNoInvtDate(user, remainBagLotNo, remainBagProductNo, '7PLD2007');
            if (!remainBagInvtResult.inDateSeq.length) {
                const InDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
                if (InDateSeqResult.error) {
                    throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
                }
                remainBagInvtResult.inDateSeq = InDateSeqResult.res;
            }

            let bodyData = [{
                'DEBUG': TEST_MODE, //(true測試/false正式)
                'SHEET_ID': sheetID,  //固定
                'SHTNO': shtNo, //getInvShtNo產生，感謝建銘哥整理
                'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
                'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
                'PRD_PC': productNo, //成品
                'PM': 'P', //(P成品/M原料)
                'PCK_KIND': remainBagWeight,
                'PCK_NO': '*',
                'MAT_PC': remainBagProductNo, //原成品
                'QTY': remainBagWeight,
                'LOT_NO': lotNo, //主排程LOT_NO
                'CCPCODE': ('pick' === pickType) ? 'B103' : 'B102', //B103改番領料、B102重工領料
                'SIGN': '-', //領料固定為負號
                'QSTATUS': 'C',
                'INDATESEQ': remainBagInvtResult.inDateSeq, //需要扣儲位帳的INDATESEQ
                'LOC': '7PLD2007', //原成品儲位
                'CREATOR': '' + user.PPS_CODE
            }];
            let apiResult = await axios.post(pickApiURL, bodyData, { proxy: false });
            console.log(`${line}-${sequence}; ${lotNo}; 殘包改番，領料改番${apiResult.data[0][2]}`);

            bodyData[0].WEIGHT = remainBagWeight;
            await storageDB.saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], 'REMAINBAG');

            //產生繳庫過帳單號
            sheetID = 'PT2';
            shtNoResult = await getInvShtNo(sheetID, date);
            if (shtNoResult.error) {
                throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
            }
            shtNo = shtNoResult.res;

            //取得該成品批號的INDATESEQ
            let inDateSeq;
            let oldInDateSeq = await storageDB.getLotNoInvtDate(user, lotNo, productNo, loc);
            if (oldInDateSeq.inDateSeq.length) {
                inDateSeq = oldInDateSeq.inDateSeq;
            } else {
                //產生儲位入庫日期序號
                let inDateSeqResult = await getInvInDateSeq(moment(date).format('YYYYMMDD'));
                if (inDateSeqResult.error) {
                    throw new Error('產生儲位入庫日期序號失敗: ' + inDateSeqResult.error);
                }
                inDateSeq = inDateSeqResult.res;
            }

            //繳庫
            bodyData = [{
                'DEBUG': TEST_MODE, //(true測試/false正式)
                'SHEET_ID': sheetID,  //固定
                'SHTNO': shtNo, //getInvShtNo產生
                'INVT_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
                'LOC_DATE': moment(date).format('YYYYMMDD'), //format=YYYYMMDD
                'PRD_PC': productNo, //成品
                'QTY': remainBagWeight, //繳庫重量
                'OLDQTY': remainBagWeight, //只要不是負數，要與QTY帶一樣的
                'SIGN': '+', //繳庫正負號
                'LOT_NO': lotNo, //主排程的批號
                'CCPCODE': ('pick' === pickType) ? 'E171' : 'E170', //E100一般繳庫、E171改番繳庫、E170重工繳庫
                'CREATOR': '' + user.PPS_CODE,
                'INDATESEQ': inDateSeq,
                'LOC': loc //主排程的SILO1
            }];
            apiResult = await axios.post(payApiURL, bodyData, { proxy: false });
            console.log(`${line}-${sequence}; ${lotNo}; 殘包改番，繳庫${apiResult.data[0][2]}`);

            bodyData[0].WEIGHT = remainBagWeight;
            await storageDB.saveERPPostingRecord(user, date, date, bodyData[0], apiResult.data[0][2], null, 'FEED');

            //紀錄領料紀錄
            sql = `
                INSERT INTO PBTC_IOT_PICKING_RECORD ( LINE, SEQUENCE, BATCH_NO, LOT_NO, SEMI_NO, MATERIAL, WEIGHT, PICK_DATE, PICK_NUM, QA_RESULT, OG, PPS_CODE, NAME, COMPANY, FIRM, STAGE )
                VALUES ( :LINE, :SEQUENCE, :BATCH_NO, :LOT_NO, :SEMI_NO, :MATERIAL, :WEIGHT, SYSDATE, :PICK_NUM, 'RE', :OG, :PPS_CODE, :NAME, :COMPANY, :FIRM, 'EXTRUSION' ) `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + remainBagLotNo },
                SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'M' }, //押出入料統一為M成品
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: remainBagProductNo },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainBagWeight) },
                PICK_NUM: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: 1 },
                OG: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'REMAINBAG' }, //重工種類
                PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });

        } else {
            throw new Error('殘包資料取得異常');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'remainBagFeeding', err);
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


//入料機變更
export async function changeFeederNo(line, sequence, oldFeederNo, newFeederNo, material, user) {
    let obj = {
        res: '',
        error: true,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //先做檢查排程是否已經啟動、是否已經入料過、是否為半成品
        sql = ` 
            SELECT 
                S0.FEEDER_NO, S0.MATERIAL, S0.SEMI_NO, S0.FEED_NUM, S1.ACT_STR_TIME
            FROM PBTC_IOT_EXTRUSION S0 
                LEFT JOIN PRO_SCHEDULE S1
                ON S0.LINE = S1.LINE
                AND S0.SEQUENCE = S1.SEQ
                AND S0.COMPANY = S1.COMPANY
                AND S0.FIRM = S1.FIRM
            WHERE S0.LINE = :LINE
            AND S0.SEQUENCE = :SEQUENCE
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            if (result.rows[0].ACT_STR_TIME) {
                obj.res = '排程已啟動';
            } else if (line + 'M1' === oldFeederNo || line + 'M1' === newFeederNo) {
                obj.res = 'M1入料機無法變更';
            } else if (line !== oldFeederNo[0] || line !== newFeederNo[0]) {
                obj.res = '入料機名稱設定錯誤';
            } else if (result.rows.filter(x => x.FEEDER_NO === newFeederNo).length) {
                obj.res = '該入料機已被使用過';
            } else if (result.rows.filter(x => 0 < x.FEED_NUM).length) {
                obj.res = '已入料過的排程';
            } else if (result.rows.filter(x => x.FEEDER_NO === oldFeederNo && (x.SEMI_NO === 'P' || x.SEMI_NO === 'G')).length) {
                obj.res = '半成品入料機不可變更';
            }

            if (obj.res.length) {
                return obj;
            } else {
                obj.error = false;
            }

            sql = `
                UPDATE PBTC_IOT_EXTRUSION
                SET FEEDER_NO = :NEW_FEEDER_NO,
                    OLD_FEEDER_NO = :OLD_FEEDER_NO,
                    CHANGE_FEEDER_TIME = SYSDATE,
                    CHANGE_FEEDER_USER = :CHANGE_FEEDER_USER
                WHERE LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND FEEDER_NO = :OLD_FEEDER_NO
                AND MATERIAL = :MATERIAL
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                NEW_FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newFeederNo },
                OLD_FEEDER_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + oldFeederNo },
                CHANGE_FEEDER_USER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) },
                MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            const commit = { autoCommit: false };
            await conn.execute(sql, params, commit);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'changeFeederNo Error', err);
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

//取得工令M1入料機的"累計"入料量
export async function getSiloWeight(line, sequence, feeders, user) {
    let obj = {
        startTime: null,
        endTime: null,
        minWeight: [], //可能回傳最多2個入料機結果
        maxWeight: [],
        error: false,
        errorMessage: '',
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //檢查是否已經由圖控按下開始了
        const sql = `
            SELECT ACT_STR_TIME, ACT_END_TIME
            FROM PRO_SCHEDULE
            WHERE LINE = :LINE
            AND SEQ = :SEQUENCE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: sequence.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);

        //可尚未結束，但不可尚未開始
        if (!result.rows[0].ACT_STR_TIME) {
            obj.errorMessage = '押出排程尚未開始';
            obj.error = true;
            return obj;
        } else {
            obj.startTime = result.rows[0].ACT_STR_TIME;
            obj.endTime = result.rows[0].ACT_END_TIME;
        }

        const mongoResult = await VisionTagsAPI.getAccumulateWeight(line, result.rows[0].ACT_STR_TIME, result.rows[0].ACT_END_TIME, false, user);
        if (mongoResult.res) {
            feeders.forEach(feeder => {
                obj.minWeight.push(mongoResult.res[`M${feeder}_minWeight`]);
                obj.maxWeight.push(mongoResult.res[`M${feeder}_maxWeight`]);
            });
        } else {
            obj.errorMessage = 'Vision Tags設定異常，或當時並未收集Tags資料';
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSiloWeight Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
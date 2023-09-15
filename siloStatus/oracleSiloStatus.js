import config from '../config.js';
import { getNowDatetimeString, getInvInDateSeq } from '../libs.js';
import oracledb from 'oracledb';
import { getOrderPacking, getOrderRemainBag, getInvtPay } from '../extrusion/oracleStorage.js';
import moment from 'moment';

//取得所有SILO與目前儲位狀況
export async function getSilos(user) {
    const obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT 
                S0.SILO_NAME, 
                S0.CAPACITY, 
                LISTAGG(S1.LOT_NO, ',' ) WITHIN GROUP ( ORDER BY S1.LOT_NO ) AS LOT_NO,
                LISTAGG(S1.PRD_PC, ',' ) WITHIN GROUP ( ORDER BY S1.PRD_PC ) AS PRD_PC,
                SUM( S1.QTY ) AS QTY,
                LISTAGG(S2.ACT_STR_TIME, ',' ) WITHIN GROUP ( ORDER BY S2.ACT_STR_TIME ) AS ACT_STR_TIME,
                LISTAGG(S2.ACT_END_TIME, ',' ) WITHIN GROUP ( ORDER BY S2.ACT_END_TIME ) AS ACT_END_TIME
            FROM PBTC_IOT_PACKING_SILO S0 
                LEFT JOIN LOCINV_D@ERPTEST S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.SILO_NAME = S1.LOC
                LEFT JOIN PRO_SCHEDULE S2
                    ON S1.COMPANY = S2.COMPANY
                    AND S1.FIRM = S2.FIRM
                    AND S1.LOT_NO = S2.LOT_NO
            WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.SILO_NAME NOT LIKE '%換包'
            GROUP BY S0.SILO_NAME, S0.CAPACITY, S0.SILO_ORDER
            ORDER BY S0.SILO_ORDER `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getSilos', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得正在使用中的SILO
export async function getUsingSilos(user) {
    const obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得各線最後一筆生產的工令
        /*
        sql = `
            SELECT S0.LINE, S0.SEQ, S0.PRD_PC, S0.LOT_NO, S0.ACT_STR_TIME, REPLACE(S0.SILO, '-', '') AS SILO
            FROM PRO_SCHEDULE S0
            WHERE S0.ACT_STR_TIME = (
                SELECT MAX(ACT_STR_TIME)
                FROM PRO_SCHEDULE
                WHERE S0.LINE = LINE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
            )
            ORDER BY SILO `;
        */
        //取得最後一個使用到各SILO的工令
        sql = `
            SELECT S0.LINE, S0.SEQ, S0.PRD_PC, S0.LOT_NO, S0.ACT_STR_TIME, REPLACE(S0.SILO, '-', '') AS SILO
            FROM PRO_SCHEDULE S0
            WHERE S0.ACT_STR_TIME = (
                SELECT MAX(ACT_STR_TIME)
                FROM PRO_SCHEDULE
                WHERE S0.SILO = SILO
                AND SILO LIKE 'S%'
                AND ACT_STR_TIME > TO_DATE('20230601', 'YYYYMMDD')
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
            )
            ORDER BY SILO `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        const schedules = result.rows;
        for (const schedule of schedules) {
            //該工令是否已經全數包裝完成
            const packingResult = await getOrderPacking(schedule.LINE, schedule.SEQ, null, null, user);
            let packingTotal = 0;
            if (packingResult.rows.length) {
                const packingData = packingResult.rows[0];
                packingTotal = packingData.TOTAL_WEIGHT;
                if (packingData.ROW_NUM && packingData.PACKING_STATUS) {
                    if (packingData.ROW_NUM === packingData.PACKING_STATUS.split('已完成').length + packingData.PACKING_STATUS.split('強制結束').length - 2) {
                        continue; //該工令已經包裝完成，不回傳SILO狀態
                    }
                }
            }

            //已繳庫多少
            const invtPayResult = await getInvtPay('lotNo', null, null, null, null, schedule.LOT_NO, user, false);
            const invtPayTotal = invtPayResult.res.length ? invtPayResult.res[0].FEED_STORAGE : 0;

            obj.res.push({
                SILO_NAME: schedule.SILO,
                PRD_PC: schedule.PRD_PC,
                LOT_NO: schedule.LOT_NO,
                PAY: invtPayTotal,
                PACK: packingTotal,
            });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getUsingSilos', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//定期抓包裝排程是否已完成且出空，將SILO儲位清除
export async function removeEmptySilo(user) {
    const obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得儲位下各個SILO當前使用的批號
        sql = `
            SELECT S0.SILO_NAME, S1.LOT_NO, S1.QTY, S2.LINE, S2.SEQ
            FROM PBTC_IOT_PACKING_SILO S0 
                LEFT JOIN LOCINV_D@ERPTEST S1
                    ON S0.COMPANY = S1.COMPANY
                    AND S0.FIRM = S1.FIRM
                    AND S0.SILO_NAME = S1.LOC
                LEFT JOIN PRO_SCHEDULE S2
                    ON S1.COMPANY = S2.COMPANY
                    AND S1.FIRM = S2.FIRM
                    AND S1.LOT_NO = S2.LOT_NO
            WHERE S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            AND S0.SILO_NAME NOT LIKE '%換包'
            AND S1.LOT_NO IS NOT NULL
            ORDER BY S0.SILO_ORDER, S1.INDATESEQ DESC `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        const silos = result.rows;

        for (const silo of silos) {
            //該工令是否已經全數包裝完成，只要包裝排程其中一筆"出空IS_EMPTYING"且全數已完成，就清空SILO
            const packingResult = await getOrderPacking(silo.LINE, silo.SEQ, null, null, user);
            if (packingResult.rows.length) {
                const packingData = packingResult.rows[0];
                if (packingData.ROW_NUM && packingData.PACKING_STATUS && packingData.IS_EMPTYING) {
                    if ((packingData.ROW_NUM === packingData.PACKING_STATUS.split('已完成').length + packingData.PACKING_STATUS.split('強制結束').length - 2)
                        &&
                        (1 === packingData.IS_EMPTYING)
                    ) {
                        console.log(`排程批號${silo.LOT_NO}於Silo${silo.SILO_NAME}出空，將清除該儲位資料`);
                        //該工令已經包裝完成，清空該SILO儲位
                        sql = ` 
                            DELETE LOCINV_D@ERPTEST
                            WHERE LOC = :LOC
                            AND COMPANY = :COMPANY
                            AND FIRM = :FIRM
                            AND WAHS = 'PT2' `;
                        params = {
                            LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + silo.SILO_NAME },
                            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        };
                        await conn.execute(sql, params, { autoCommit: true });

                        //是否已經存在該批號的殘包，不存在才新增殘包儲位
                        sql = `
                            SELECT LOT_NO
                            FROM LOCINV_D@ERPTEST
                            WHERE COMPANY = :COMPANY
                            AND FIRM = :FIRM
                            AND DEPT = :DEPT
                            AND WAHS = :WAHS
                            AND PRD_PC = :PRD_PC
                            AND LOT_NO = :LOT_NO
                            AND LOC = :LOC
                            AND REMARK = :REMARK `;
                        params = {
                            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                            WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'PT2' }, //固定
                            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingData.PRD_PC },
                            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: silo.LOT_NO },
                            LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '7PLD2007' }, //固定
                            REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '系統包殘' },
                        };
                        const remainBagResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                        if (!remainBagResult.rows.length) {
                            console.log('建立殘包儲位'); //測試

                            //取得殘包相關資料
                            let remainBagWeight = 0;
                            const remainBagData = await getOrderRemainBag(silo.LINE, silo.SEQ, user);
                            if (remainBagData.rows.length) {
                                remainBagWeight = remainBagData.rows.length ? remainBagData.rows[0].REMAIN_BAG_WEIGHT : 0;
                            }

                            //取得殘包儲位INDATESEQ
                            const InDateSeqResult = await getInvInDateSeq(moment().format('YYYYMMDD'));
                            if (InDateSeqResult.error) {
                                throw new Error('產生儲位入庫日期序號失敗: ' + InDateSeqResult.error);
                            }

                            //將出空的殘包塞入儲位
                            sql = `
                                INSERT INTO LOCINV_D@ERPTEST ( 
                                    COMPANY, FIRM, DEPT, WAHS, PRD_PC, LOT_NO, UNIT, PCK_NO,
                                    LOC, IN_DATE, QTY, INDATESEQ, LOCDATE, REMARK )
                                VALUES ( 
                                    :COMPANY, :FIRM, :DEPT, :WAHS, :PRD_PC, :LOT_NO, 'KG', '*', 
                                    :LOC, SYSDATE, :QTY, :INDATESEQ, SYSDATE, :REMARK ) `;
                            params = {
                                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                                WAHS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: 'PT2' }, //固定
                                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packingData.PRD_PC },
                                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: silo.LOT_NO },
                                LOC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '7PLD2007' }, //固定
                                QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(remainBagWeight) },
                                INDATESEQ: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: InDateSeqResult.res },
                                REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: `系統包殘 ${remainBagWeight}Kg` },
                            };
                            await conn.execute(sql, params, { autoCommit: true });
                        }
                    }
                }
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeEmptySilo', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
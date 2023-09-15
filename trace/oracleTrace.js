import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import oracledb from 'oracledb';
import moment from 'moment';

//使用包裝棧板編號查詢原料LOT_NO
export async function getMaterials(packPalletNo, user) {
    let obj = {
        res: [],
        seqStart: '',
        seqEnd: '',
        productNo: '',
        lotNo: '',
        line: '',
        sequence: '',
        startTime: '',
        endTime: '',
        totalWeight: 0, //已包裝量
        weightStart: 0, //包裝區間
        weightEnd: 0, //包裝區間
        error: false,
    };

    let conn;
    let sql;
    let params;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        sql = `
            SELECT 
                DET.DETAIL_SEQ_START, DET.DETAIL_SEQ_END, DET.PACKING_SEQ,
                PRO.PRD_PC, PRO.LOT_NO, PRO.LINE, PRO.SEQ, PRO.ACT_STR_TIME, PRO.ACT_END_TIME, PRO.PRO_WT,
                ( 
                    SELECT MAX(DETAIL_SEQ_END)
                    FROM PBTC_IOT_PACKING_DETAIL
                    WHERE PACKING_SEQ = DET.PACKING_SEQ
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM 
                    GROUP BY PACKING_SEQ
                ) AS MAX_DETAIL_SEQ_END
            FROM PBTC_IOT_PACKING_DETAIL DET
                LEFT JOIN PBTC_IOT_PACKING_SCHEDULE SCH ON DET.PACKING_SEQ = SCH.PACKING_SEQ
                LEFT JOIN PRO_SCHEDULE PRO ON SCH.PRO_SCHEDULE_UKEY = PRO.UKEY
            WHERE DET.PALLET_NO = :PALLET_NO
            AND DET.COMPANY = :COMPANY
            AND DET.FIRM = :FIRM `;
        params = {
            PALLET_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: packPalletNo },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            let data = result.rows[0];
            if (!data.ACT_END_TIME) {
                throw new Error('該工令尚未完成');
            }

            data.ACT_END_TIME = data.ACT_END_TIME || new Date();
            obj.seqStart = data.DETAIL_SEQ_START;
            obj.seqEnd = data.DETAIL_SEQ_END;
            obj.productNo = data.PRD_PC;
            obj.lotNo = data.LOT_NO;
            obj.line = data.LINE;
            obj.sequence = data.SEQ;
            obj.startTime = (1 === data.DETAIL_SEQ_START) ? moment(data.ACT_STR_TIME) : moment(data.ACT_STR_TIME).add((data.ACT_END_TIME - data.ACT_STR_TIME) * (data.DETAIL_SEQ_START - 1 / data.MAX_DETAIL_SEQ_END));
            obj.endTime = moment(data.ACT_STR_TIME).add((data.ACT_END_TIME - data.ACT_STR_TIME) * (data.DETAIL_SEQ_END / data.MAX_DETAIL_SEQ_END));

            //取得該工令的已包裝量
            //const totalWeightResult = await storageDB.getOrderPacking(data.LINE, data.SEQ, null, null, user);

            //改以領料累計量取代已包裝量
            sql = `
                SELECT SUM(WEIGHT) AS TOTAL_WEIGHT
                FROM PBTC_IOT_PICKING_RECORD 
                WHERE LINE = :LINE 
                AND SEQUENCE = :SEQUENCE 
                AND STAGE = 'EXTRUSION' 
                AND COMPANY = :COMPANY 
                AND FIRM = :FIRM `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: data.LINE },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(data.SEQ) },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            const totalWeightResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

            if (totalWeightResult.rows.length) {
                obj.totalWeight = totalWeightResult.rows[0].TOTAL_WEIGHT;
                obj.weightStart = parseInt((data.DETAIL_SEQ_START - 1) * totalWeightResult.rows[0].TOTAL_WEIGHT / data.MAX_DETAIL_SEQ_END);
                obj.weightEnd = parseInt(data.DETAIL_SEQ_END * totalWeightResult.rows[0].TOTAL_WEIGHT / data.MAX_DETAIL_SEQ_END);

                //由領料區間找累計使用量
                sql = `
                    SELECT SEMI_NO, BATCH_NO, LOT_NO, LOC_LOT_NO, MATERIAL, SUM(WEIGHT) AS WEIGHT
                    FROM (
                        SELECT 
                            REC.SEMI_NO, REC.BATCH_NO, REC.LOT_NO, LOC.LOT_NO AS LOC_LOT_NO, REC.MATERIAL, REC.WEIGHT, REC.PICK_DATE,
                            SUM(REC.WEIGHT) OVER (ORDER BY REC.PICK_DATE) AS ACCUMULATE_WEIGHT
                        FROM PBTC_IOT_PICKING_RECORD REC
                            LEFT JOIN LOCINV_D LOC ON REC.BATCH_NO = LOC.PAL_NO
                        WHERE REC.LINE = :LINE
                        AND REC.SEQUENCE = :SEQUENCE
                        AND REC.STAGE = 'EXTRUSION'
                        AND REC.COMPANY = :COMPANY
                        AND REC.FIRM = :FIRM
                    )
                    WHERE ACCUMULATE_WEIGHT >= :START_WEIGHT 
                    AND ACCUMULATE_WEIGHT - WEIGHT <= :END_WEIGHT
                    GROUP BY SEMI_NO, BATCH_NO, LOT_NO, LOC_LOT_NO, MATERIAL `;
                params = {
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: data.LINE },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(data.SEQ) },
                    START_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(obj.weightStart) },
                    END_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(obj.weightEnd) },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };

            } else {
                //由時間區間內找工令領料紀錄
                sql = `
                    SELECT REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, REC.STAGE, REC.SEMI_NO, LOC.LOT_NO AS LOC_LOT_NO
                    FROM PBTC_IOT_PICKING_RECORD REC
                        LEFT JOIN LOCINV_D LOC ON REC.BATCH_NO = LOC.PAL_NO
                    WHERE REC.LINE = :LINE
                    AND REC.SEQUENCE = :SEQUENCE
                    AND REC.PICK_DATE >= :START_TIME
                    AND REC.PICK_DATE <= :END_TIME
                    AND REC.STAGE = 'EXTRUSION'
                    AND REC.COMPANY = :COMPANY
                    AND REC.FIRM = :FIRM
                    GROUP BY REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, REC.STAGE, REC.SEMI_NO, LOC.LOT_NO `;
                params = {
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: data.LINE },
                    SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(data.SEQ) },
                    START_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: moment(obj.startTime).toDate() },
                    END_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: moment(obj.endTime).toDate() },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                };
            }
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

            for (const row of result.rows) {
                //若是入拌粉半成品，展開該拌粉半成品領料紀錄
                if ('P' === row.SEMI_NO || 'G' === row.SEMI_NO) {
                    sql = `
                        SELECT 
                            REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, REC.STAGE, LOC.LOT_NO AS LOC_LOT_NO, 
                            SUM(REC.WEIGHT) AS WEIGHT, 
                            GET_QC_RESULTM(:COMPANY, :FIRM, :DEPT, REC.MATERIAL, REC.LOT_NO) AS QC_RESULT
                        FROM PBTC_IOT_PICKING_RECORD REC
                            LEFT JOIN LOCINV_D LOC ON REC.BATCH_NO = LOC.PAL_NO
                        WHERE REC.LINE = :LINE
                        AND REC.SEQUENCE = :SEQUENCE
                        AND REC.STAGE = 'MIX'
                        AND REC.SEMI_NO = :SEMI_NO
                        AND REC.BATCH_SEQ_START <= :BATCH_SEQ
                        AND REC.BATCH_SEQ_END >= :BATCH_SEQ
                        AND REC.COMPANY = :COMPANY
                        AND REC.FIRM = :FIRM
                        GROUP BY REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, REC.STAGE, LOC.LOT_NO `;
                    params = {
                        LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: data.LINE },
                        SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: data.SEQ },
                        SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: row.SEMI_NO },
                        BATCH_SEQ: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(row.BATCH_NO.split('-')[1]) }, //H1745-4 >> 4
                        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                    };
                    result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
                    obj.res = result.rows;
                }
            }

            //由押出入料管制表查詢工令M1入料機使用的原料批號
            sql = `
                SELECT BATCH_NO, LOT_NO, MATERIAL, SUM(WEIGHT) AS WEIGHT
                FROM PBTC_IOT_PICKING_RECORD
                WHERE SILO IS NOT NULL
                AND LINE = :LINE
                AND SEQUENCE = :SEQUENCE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                GROUP BY BATCH_NO, LOT_NO, MATERIAL `;
            params = {
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: data.LINE },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: data.SEQ },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            if (result.rows.length) {
                obj.res.push(result.rows[0]);
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMaterials', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//稽核專用，使用批號、成品簡碼查詢各種原料領用記錄(重複的批號整在一起)
export async function getMaterialsAudit(queryType, lotNo, productNo, startDate, endDate, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT
                REC.LINE, REC.SEQUENCE, REC.BATCH_NO, REC.MATERIAL, REC.LOT_NO, SUM(REC.WEIGHT) AS WEIGHT
            FROM PBTC_IOT_PICKING_RECORD REC LEFT JOIN PRO_SCHEDULE PRO
                ON REC.LINE = PRO.LINE
                AND REC.SEQUENCE = PRO.SEQ
                AND REC.COMPANY = PRO.COMPANY
                AND REC.FIRM = PRO.FIRM
            WHERE REC.COMPANY = :COMPANY
            AND REC.FIRM = :FIRM
            AND REC.OG IS NULL
            AND REC.LOT_NO IS NOT NULL `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('lotNo' === queryType) {
            sql += ' AND PRO.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };
        } else if ('productNo' === queryType) {
            sql += ' AND PRO.PRD_PC = :PRD_PC AND REC.PICK_DATE BETWEEN :START_DATE AND :END_DATE ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo };
            params['START_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(startDate) };
            params['END_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(endDate) };
        }

        sql += ` 
            GROUP BY REC.LINE, REC.SEQUENCE, REC.BATCH_NO, REC.MATERIAL, REC.LOT_NO
            ORDER BY LINE, SEQUENCE, MATERIAL`;
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getMaterialsAudit', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//使用原料LotNo查有使用此LotNo的包裝成品
export async function getPackPalletNo(queryType, lotNo, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //空輸原料，從押出入料管制表抓
        if ('silo' === queryType) {
            sql = `
                SELECT 
                    REC.LOT_NO, REC.SILO, REC.LINE, REC.SEQUENCE, SCH.PRD_PC, DET.PALLET_NO
                FROM PBTC_IOT_PICKING_RECORD REC
                    LEFT JOIN PBTC_IOT_PACKING_SCHEDULE SCH ON REC.LINE = SCH.PRO_SCHEDULE_LINE AND REC.SEQUENCE = SCH.PRO_SCHEDULE_SEQ
                    LEFT JOIN PBTC_IOT_PACKING_DETAIL DET ON SCH.PACKING_SEQ = DET.PACKING_SEQ
                WHERE REC.LOT_NO = :LOT_NO
                AND REC.SILO IS NOT NULL --區分空輸
                AND DET.PALLET_NO IS NOT NULL --僅顯示已經包裝好的
                AND REC.COMPANY = :COMPANY
                AND REC.FIRM = :FIRM
                GROUP BY REC.LOT_NO, REC.SILO, REC.LINE, REC.SEQUENCE, SCH.PRD_PC, DET.PALLET_NO
                ORDER BY REC.LINE, REC.SEQUENCE, DET.PALLET_NO `;
            params = {
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };

        } else {
            //原料LotNo下會有多個PAL_NO
            sql = `
                SELECT 
                    REC.LINE, REC.SEQUENCE, REC.BATCH_SEQ_START, REC.BATCH_SEQ_END, REC.STAGE, SCH.PRD_PC, DET.PALLET_NO
                FROM PBTC_IOT_PICKING_RECORD REC
                    LEFT JOIN PBTC_IOT_PACKING_SCHEDULE SCH ON REC.LINE = SCH.PRO_SCHEDULE_LINE AND REC.SEQUENCE = SCH.PRO_SCHEDULE_SEQ
                    LEFT JOIN PBTC_IOT_PACKING_DETAIL DET ON SCH.PACKING_SEQ = DET.PACKING_SEQ
                WHERE REC.LOT_NO = :LOT_NO
                AND REC.SILO IS NULL --區分空輸
                AND DET.PALLET_NO IS NOT NULL --僅顯示已經包裝好的
                AND REC.COMPANY = :COMPANY
                AND REC.FIRM = :FIRM
                GROUP BY REC.LINE, REC.SEQUENCE, REC.BATCH_SEQ_START, REC.BATCH_SEQ_END, REC.STAGE, SCH.PRD_PC, DET.PALLET_NO
                ORDER BY REC.LINE, REC.SEQUENCE, DET.PALLET_NO `;
            params = {
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
        }

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPackPalletNo', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//工令查詢所有棧板使用紀錄
export async function getPalletPicked(queryType, searchDate, line, sequence, lotNo, productNo, startDate, endDate, pickStage, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT
                REC.LINE, REC.SEQUENCE, REC.BATCH_NO, REC.MATERIAL, REC.WEIGHT, REC.PICK_NUM, REC.LOT_NO, REC.QA_RESULT,
                LOC.LOT_NO AS LOC_LOT_NO, REC.PICK_DATE, REC.PPS_CODE, REC.NAME, REC.STAGE, REC.BATCH_SEQ_START, REC.BATCH_SEQ_END,
                PRO.LINE, PRO.SEQ, PRO.LOT_NO AS PRO_LOT_NO
            FROM PBTC_IOT_PICKING_RECORD REC
                LEFT JOIN LOCINV_D LOC
                    ON REC.BATCH_NO = LOC.PAL_NO
                LEFT JOIN PRO_SCHEDULE PRO
                    ON REC.LINE = PRO.LINE
                    AND REC.SEQUENCE = PRO.SEQ
                    AND REC.COMPANY = PRO.COMPANY
                    AND REC.FIRM = PRO.FIRM
            WHERE REC.SILO IS NULL --濾掉空輸樹酯
            AND REC.COMPANY = :COMPANY
            AND REC.FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('order' === queryType) {
            sql += ' AND REC.LINE = :LINE AND REC.SEQUENCE = :SEQUENCE ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
            params['SEQUENCE'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) };
        } else if ('date' === queryType) {
            sql += ' AND TO_CHAR( REC.PICK_DATE, \'YYYYMMDD\' ) = :PICK_DATE ';
            params['PICK_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(searchDate).format('YYYYMMDD') };
        } else if ('lotNo' === queryType) {
            sql += ' AND PRO.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };
        } else if ('productNo' === queryType) {
            sql += ' AND PRO.PRD_PC = :PRD_PC AND REC.PICK_DATE BETWEEN :START_DATE AND :END_DATE ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo };
            params['START_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(startDate) };
            params['END_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(endDate) };
        }

        if ('MIX' === pickStage || 'EXTRUSION' === pickStage) {
            sql += ' AND REC.STAGE = :PICK_STAGE  ';
            params['PICK_STAGE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickStage };
        } else if ('ALL' === pickStage) {
            sql += ' AND REC.LOT_NO IS NOT NULL '; //濾掉半成品
        } else {
            sql += ' AND REC.OG = :OG ';
            params['OG'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: pickStage };
        }

        sql += ' ORDER BY REC.PICK_DATE ';
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;

        //因M1樹酯為每十分鐘自動領一次，細項太多故單獨拉出來顯示
        if (result.rows.length  && ('EXTRUSION' === pickStage || 'ALL' === pickStage)) {
            sql = `
                SELECT REC.LINE, REC.SEQUENCE, REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, SUM(WEIGHT) AS WEIGHT, PRO.LOT_NO AS PRO_LOT_NO
                FROM PBTC_IOT_PICKING_RECORD REC
                    LEFT JOIN LOCINV_D LOC
                        ON REC.BATCH_NO = LOC.PAL_NO
                    LEFT JOIN PRO_SCHEDULE PRO
                        ON REC.LINE = PRO.LINE
                        AND REC.SEQUENCE = PRO.SEQ
                        AND REC.COMPANY = PRO.COMPANY
                        AND REC.FIRM = PRO.FIRM
                WHERE REC.SILO IS NOT NULL
                AND REC.COMPANY = :COMPANY
                AND REC.FIRM = :FIRM `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };

            if ('order' === queryType) {
                sql += ' AND REC.LINE = :LINE AND REC.SEQUENCE = :SEQUENCE ';
                params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line };
                params['SEQUENCE'] = { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(sequence) };
            } else if ('date' === queryType) {
                sql += ' AND TO_CHAR( REC.PICK_DATE, \'YYYYMMDD\' ) = :PICK_DATE ';
                params['PICK_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(searchDate).format('YYYYMMDD') };
            } else if ('lotNo' === queryType) {
                sql += ' AND PRO.LOT_NO = :LOT_NO ';
                params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lotNo };
            } else if ('productNo' === queryType) {
                sql += ' AND PRO.PRD_PC = :PRD_PC AND REC.PICK_DATE BETWEEN :START_DATE AND :END_DATE ';
                params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo };
                params['START_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(startDate) };
                params['END_DATE'] = { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date(endDate) };
            }

            sql += ' GROUP BY REC.LINE, REC.SEQUENCE, REC.BATCH_NO, REC.LOT_NO, REC.MATERIAL, PRO.LOT_NO ';
            const resinResult = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
            obj.res.push(...resinResult.rows);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPalletPicked', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
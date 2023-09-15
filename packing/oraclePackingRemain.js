import config from '../config.js';
import * as libs from '../libs.js';
import moment from 'moment';
import oracledb from 'oracledb';
import FormData from 'form-data';
import axios from 'axios';
import { getRemainderLabelNo } from '../packing/packingWork.js';
import * as Mailer from '../mailer.js';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const ExcelJS = require('exceljs');

const axiosConfig = {
    proxy: false,
    timeout: 5000,
};

//查詢殘包儲位資訊
export async function getLONOData(user, LONO) {
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
            SELECT LONO, PRD_PC, LOT_NO, WEIGHT, OPNO
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND STATUS = '1'
            AND IO = 'I' `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO }
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getLONOData', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢殘包資訊
export async function getBagData(user, OPNO) {
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
            SELECT PRD_PC, LOT_NO, WEIGHT, OPNO, LONO
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND OPNO = :OPNO `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getBagData', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//確認ProSchedule的規格和批號
export async function confirmProSchedule(user, bagPRD_PC, bagLOT_NO) {
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
            SELECT LOT_NO, PRD_PC
            FROM PRO_SCHEDULE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PRD_PC = :PRD_PC 
            AND LOT_NO = :LOT_NO`;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + bagPRD_PC },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + bagLOT_NO },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'confirmProSchedule', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//列印殘包標籤
export async function printBagLabel(user, PRD_PC, LOT_NO, property, weight, printerIP) {
    const obj = {
        res: [],
        error: null,
    };
    try {
        const opno = await getRemainderLabelNo(user);

        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);
        formData.append('PPS_CODE', user.PPS_CODE);
        formData.append('PRINTER_IP', printerIP); //標籤機IP
        formData.append('LOT_NO', LOT_NO);
        formData.append('PRD_PC', PRD_PC);
        formData.append('PACK_NO', property); //包裝性質代號 P40
        formData.append('OPNO', opno.res); //殘包編號
        formData.append('WEIGHT', weight); //殘包重量
        formData.append('CRT_TIME', moment().format('YYYY/MM/DD HH:mm:ss')); //列印時間
        formData.append('TAG_KIND', 'RM'); //標籤種類
        formData.append('REPRINT', 'false'); //是否為重印

        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/printRemainingPbtTag';

        await axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            if ('PRINT_OK' === res.data) {
                obj.res = true;
            } else {
                obj.error = res.data;
            }
            return res.data;
        }).catch(err => {
            throw err;
        });
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'printBagLabel', err);
    }
    return obj;
}

//列印格位標籤
export async function printLonoLabel(user, printerIP, printData) {
    const obj = {
        res: [],
        error: null,
    };

    const formData = new FormData();

    formData.append('COMPANY', user.COMPANY);
    formData.append('FIRM', user.FIRM);
    formData.append('DEPT', user.DEPT);
    formData.append('PRINTER_IP', printerIP);
    formData.append('TAG_KIND', 'RM_LONO');
    formData.append('LONO_1', '' + printData + '01');
    formData.append('LONO_2', '' + printData + '02');
    formData.append('LONO_3', '' + printData + '03');
    formData.append('LONO_4', '' + printData + '04');

    const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/printLonoTag';

    await axios.post(url, formData, {
        ...axiosConfig,
        headers: {
            'Content-Type': 'multipart/form-data',
        },
    }).then(res => {
        if ('PRINT_OK' === res.data) {
            obj.res = true;
        } else {
            obj.error = res.data;
        }
        return res.data;
    }).catch(err => {
        console.error(libs.getNowDatetimeString(), 'printLonoLabel', err);
    });
    return obj;
}

//列印殘包標籤時記錄資訊至BAGINFO
export async function storeBagInfo(user, PRD_PC, LOT_NO, weight, opno) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {

        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const bagSql = `
            INSERT INTO RM_BAGINFO (COMPANY, FIRM, DEPT, PRD_PC, LOT_NO, WEIGHT, OPNO, CREATOR )
            VALUES ( :COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO, :WEIGHT, :OPNO, :CREATOR ) `;
        const bagParams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + PRD_PC },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LOT_NO },
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        await conn.execute(bagSql, bagParams, { autoCommit: true });
    } catch (err) {
        console.log(libs.getNowDatetimeString(), 'storeBagInfo', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//入庫時確認BAGINFO有無這筆記錄
export async function updateBagInfo(user, doingType, prd_pc, lot_no, weight, LONO, OPNO, reason) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        if ('IN' === doingType) {
            sql = `
                INSERT INTO RM_BAGINFO ( COMPANY, FIRM, DEPT, PRD_PC, LOT_NO, LONO, WEIGHT, OPNO, STATUS, CREATOR )
                VALUES ( :COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO, :LONO, :WEIGHT, :OPNO, '1', :CREATOR) `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prd_pc },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lot_no },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            };
        } else if ('OUT' === doingType) {
            sql = `
                INSERT INTO RM_BAGINFO (COMPANY, FIRM, DEPT, OPNO, PRD_PC, LOT_NO, LONO, WEIGHT, STATUS, CREATOR, REASON ) 
                VALUES ( :COMPANY, :FIRM, :DEPT, :OPNO, :PRD_PC, :LOT_NO, :LONO, :WEIGHT, '0', :CREATOR, :REASON ) `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prd_pc },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lot_no },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                REASON: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (1 === reason) ? '押出回摻' : (2 === reason) ? '包裝回摻' : (3 === reason) ? '重工去化' : null },
            };
        } else if ('manualOut' === doingType) {
            sql = `
                INSERT INTO RM_BAGINFO (COMPANY, FIRM, DEPT, OPNO, PRD_PC, LOT_NO, LONO, WEIGHT, STATUS, CREATOR, REMARK, REASON ) 
                VALUES ( :COMPANY, :FIRM, :DEPT, :OPNO, :PRD_PC, :LOT_NO, :LONO, :WEIGHT, '0', :CREATOR, :REMARK, :REASON ) `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prd_pc },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lot_no },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '手動出庫' },
                REASON: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('1' === reason) ? '押出回摻' : ('2' === reason) ? '包裝回摻' : ('3' === reason) ? '重工去化' : null },
            };
        } else if ('manualDelete' === doingType) {
            sql = `
                INSERT INTO RM_BAGINFO (COMPANY, FIRM, DEPT, OPNO, PRD_PC, LOT_NO, LONO, WEIGHT, STATUS, CREATOR, REMARK ) 
                VALUES ( :COMPANY, :FIRM, :DEPT, :OPNO, :PRD_PC, :LOT_NO, :LONO, :WEIGHT, '0', :CREATOR, :REMARK ) `;
            params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + prd_pc },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lot_no },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
                REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '刪除資料' },
            };
        }
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.log(libs.getNowDatetimeString(), 'updateBagInfo', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }
}

//殘包入庫-記錄入庫時間日期、人員，新增格位資料
export async function bagInStorage(user, OPNO, LONO) {
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
            MERGE INTO RM_STGFLD USING DUAL ON (
                COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND OPNO = :OPNO
            )
            WHEN MATCHED THEN
                UPDATE SET
                    IO = 'I',
                    STATUS = 1,
                    LONO = :LONO,
                    INV_DATE = SYSDATE,
                    PPS_CODE = :PPS_CODE `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },  //殘包標籤編號
        };

        const LOCSql = `
            UPDATE RM_LOC
            SET STATUS = '1', 
                LTIM = SYSDATE
            WHERE STATUS = '0'
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO `;
        const LOCParams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
        };

        //先檢查格位狀態
        const LOCResult = await conn.execute(LOCSql, LOCParams, { autoCommit: false });
        if (LOCResult.rowsAffected) {
            const STGResult = await conn.execute(sql, params, { autoCommit: false });
            if (STGResult.rowsAffected) {
                await conn.commit();
            } else {
                await errorRecord(user, 'IN', LONO, OPNO);
                throw new Error('STG入庫異常');
            }
        } else {
            await errorRecord(user, 'IN', LONO, OPNO);
            throw new Error(`LOC入庫異常，${LONO}已被使用中`);
        }

    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'bagInStorage', OPNO, LONO, err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//殘包出庫-紀錄出庫時間日期、人員，清除格位資料 
export async function bagOutStorage(user, OPNO, LONO, reason) {
    const obj = {
        res: [],
        error: null,
    };
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const STGSql = `
            UPDATE RM_STGFLD
                SET IO = 'O',
                    STATUS = '0',
                    PPS_CODE = :PPS_CODE,
                    REASON = :REASON,
                    INV_DATE = SYSDATE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND OPNO = :OPNO
            AND IO = 'I' `;
        const STGparams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },  //殘包標籤編號
            REASON: {
                dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('1' === reason || 1 === reason) ? '押出回摻'
                    : ('2' === reason || 2 === reason) ? '包裝回摻'
                        : ('3' === reason || 3 === reason) ? '重工去化' : null
            },
        };

        const LOCSql = `
            UPDATE RM_LOC
                SET STATUS = '0',
                    LTIM = SYSDATE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND STATUS = '1' `;
        const LOCParams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
        };

        const LOCResult = await conn.execute(LOCSql, LOCParams, { autoCommit: false });

        //先檢查格位狀態
        if (LOCResult.rowsAffected) {
            const STGResult = await conn.execute(STGSql, STGparams, { autoCommit: false });
            if (STGResult.rowsAffected) {
                await conn.commit();
            } else {
                await errorRecord(user, 'OUT', LONO, OPNO);
                throw new Error(`STG出庫異常，${LONO}中目前無${OPNO}標籤`);
            }
        } else {
            await errorRecord(user, 'OUT', LONO, OPNO);
            throw new Error(`LOC出庫異常，${LONO}未被使用`);
        }

    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'bagOutStorage', LONO, OPNO, err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//出入庫異常紀錄
export async function errorRecord(user, doingType, LONO, OPNO) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            UPDATE RM_BAGINFO
                SET REMARK = :REMARK
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND OPNO = :OPNO
            AND LONO = :LONO
            AND STATUS = :STATUS `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + OPNO },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO },
            REMARK: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('IN' === doingType) ? '入庫異常' : ('OUT' === doingType) ? '出庫異常' : null },
            STATUS: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ('IN' === doingType) ? '1' : ('OUT' === doingType) ? '0' : null },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'errorRecord', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢殘包進出記錄
export async function queryStatus(user, dateStart, dateEnd, PRD_PC, LOT_NO) {
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
            SELECT DISTINCT A.PRD_PC, A.LOT_NO, A.LONO, A.INV_DATE, A.WEIGHT, A.STATUS, A.REMARK, A.CREATOR,
                B.PPS_CODE, NVL(B.NAME, ' ') AS NAME, 
                NVL(C.CNAME, ' ') AS CNAME
            FROM RM_BAGINFO A
                LEFT JOIN PERSON_FULL B
                    ON A.CREATOR = B.PPS_CODE
                LEFT JOIN PERSON C
                    ON A.CREATOR = C.NAME
            WHERE A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
            AND TO_CHAR( A.INV_DATE, 'YYYYMMDD' ) >= :START_DATE
            AND TO_CHAR( A.INV_DATE, 'YYYYMMDD' ) <= :END_DATE `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + dateStart },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + dateEnd },
        };

        if ('*' !== PRD_PC) {
            sql += ' AND PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + PRD_PC };
        }
        if ('*' !== LOT_NO) {
            sql += ' AND LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LOT_NO };
        }

        sql += ' ORDER BY A.LONO, A.PRD_PC, A.INV_DATE ';

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryStatus', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢印標籤未入庫殘包
export async function queryErrorStock(user, PRD_PC, LOT_NO, startDate, endDate) {
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
            SELECT *
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND STATUS IS NULL
            AND TO_CHAR( INV_DATE, 'YYYYMMDD' ) >= :START_DATE
            AND TO_CHAR( INV_DATE, 'YYYYMMDD' ) <= :END_DATE `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        if ('*' !== PRD_PC) {
            sql += ' AND PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + PRD_PC };
        }
        if ('*' !== LOT_NO) {
            sql += ' AND LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LOT_NO };
        }

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryErrorStock', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢殘包庫存(規格/批號/格位)
export async function queryStock(user, PRD_PC, LOT_NO, LONO) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    let newSql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        newSql = `
        SELECT A.LONO, A.PRD_PC, A.LOT_NO, A.WEIGHT, A.INV_DATE, A.PPS_CODE, A.OPNO,
            B.LINE, B.SEQ, B.ACT_STR_TIME, MIN(B.SEQ),
            C.PRO_SCHEDULE_LINE, C.PRO_SCHEDULE_SEQ, C.PACKING_DATE, C.PACKING_STATUS,
            CONCAT(MIN_SEQ_TABLE.LINE, MIN_SEQ_TABLE.MIN_SEQ) AS PRO_SAMELINE,
            CONCAT(MIN_SEQ_TABLE_NEW.LINE, MIN_SEQ_TABLE_NEW.MIN_SEQ) AS PRO_DIFFERENTLINE,
            MIN_SEQ_TABLE.LINE AS MIN_LINE, MIN_SEQ_TABLE.MIN_SEQ AS MIN_SEQ,
            CONCAT(MIN_SEQ_TABLE_3.LINE, MIN_SEQ_TABLE_3.SEQ) AS NEXT_PRO,
            CONCAT(MIN_SEQ_TABLE_3_DIFF.LINE, MIN_SEQ_TABLE_3_DIFF.SEQ) AS NEXT_PRO_DIFF
        FROM RM_STGFLD A
            LEFT JOIN PRO_SCHEDULE B
                ON A.PRD_PC = B.PRD_PC
                    AND A.COMPANY = B.COMPANY
                    AND A.FIRM = B.FIRM
                    AND A.DEPT = B.DEPT
                    AND (B.ACT_STR_TIME IS NULL OR TO_CHAR(B.ACT_STR_TIME, 'YYYYMMDD') >= '20230614')
                    AND TO_CHAR(B.STR_PRO_TIME, 'YYYYMMDD') >= '20230101'
                    AND B.ACT_END_TIME IS NULL
            LEFT JOIN PBTC_IOT_PACKING_SCHEDULE C
                ON A.PRD_PC = C.PRD_PC
                AND A.COMPANY = C.COMPANY
                AND A.FIRM = C.FIRM
                AND A.DEPT = C.DEPT
                AND TO_CHAR(C.PACKING_DATE, 'YYYYMMDD') >= '20230714'
                AND C.PACKING_FINISH_TIME IS NULL
                AND C.DELETE_TIME IS NULL
        LEFT JOIN (
                SELECT LINE, MIN(SEQ) AS MIN_SEQ, PRD_PC
                FROM PRO_SCHEDULE
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND TO_CHAR(STR_PRO_TIME, 'YYYYMMDD') >= '20230101'
                AND ACT_STR_TIME IS NULL
                AND ACT_END_TIME IS NULL
                GROUP BY LINE, PRD_PC
        ) MIN_SEQ_TABLE 
                ON B.LINE = MIN_SEQ_TABLE.LINE 
                AND B.SEQ = MIN_SEQ_TABLE.MIN_SEQ
                AND SUBSTR(A.LOT_NO, 8, 1) = MIN_SEQ_TABLE.LINE
                AND B.PRD_PC = MIN_SEQ_TABLE.PRD_PC
        LEFT JOIN (
                SELECT LINE, MIN(SEQ) AS MIN_SEQ, PRD_PC
                FROM PRO_SCHEDULE
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND TO_CHAR(STR_PRO_TIME, 'YYYYMMDD') >= '20230101'
                AND ACT_STR_TIME IS NULL
                AND ACT_END_TIME IS NULL
                GROUP BY LINE, PRD_PC
        ) MIN_SEQ_TABLE_NEW 
                ON B.LINE = MIN_SEQ_TABLE.LINE 
                AND B.SEQ = MIN_SEQ_TABLE.MIN_SEQ
                AND B.PRD_PC = MIN_SEQ_TABLE.PRD_PC
        LEFT JOIN (
            SELECT PS.LINE, PS.SEQ, PS.PRD_PC
            FROM PRO_SCHEDULE PS
            INNER JOIN (
                SELECT LINE, MIN(SEQ) AS MIN_SEQ
                FROM PRO_SCHEDULE
                WHERE COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT
                    AND TO_CHAR(STR_PRO_TIME, 'YYYYMMDD') >= '20230101'
                    AND ACT_STR_TIME IS NULL
                    AND ACT_END_TIME IS NULL
                GROUP BY LINE
            ) MIN_SEQ_TABLE 
                ON PS.LINE = MIN_SEQ_TABLE.LINE 
                AND PS.SEQ = MIN_SEQ_TABLE.MIN_SEQ
        )MIN_SEQ_TABLE_3
                ON B.PRD_PC = MIN_SEQ_TABLE_3.PRD_PC
                AND SUBSTR(A.LOT_NO, 8, 1) = MIN_SEQ_TABLE_3.LINE
        LEFT JOIN (
            SELECT PS.LINE, PS.SEQ, PS.PRD_PC
            FROM PRO_SCHEDULE PS
            INNER JOIN (
                SELECT LINE, MIN(SEQ) AS MIN_SEQ
                FROM PRO_SCHEDULE
                WHERE COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT
                    AND TO_CHAR(STR_PRO_TIME, 'YYYYMMDD') >= '20230101'
                    AND ACT_STR_TIME IS NULL
                    AND ACT_END_TIME IS NULL
                GROUP BY LINE
            ) MIN_SEQ_TABLE 
                ON PS.LINE = MIN_SEQ_TABLE.LINE 
                AND PS.SEQ = MIN_SEQ_TABLE.MIN_SEQ
        )MIN_SEQ_TABLE_3_DIFF
                ON B.PRD_PC = MIN_SEQ_TABLE_3_DIFF.PRD_PC
        WHERE A.STATUS = '1'
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT `;

        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        };

        if ('*' !== PRD_PC) {
            newSql += ' AND A.PRD_PC = :PRD_PC ';
            params['PRD_PC'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + PRD_PC };
        }
        if ('*' !== LOT_NO) {
            newSql += ' AND A.LOT_NO = :LOT_NO ';
            params['LOT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LOT_NO };
        }
        if ('*' !== LONO) {
            newSql += ' AND A.LONO = :LONO ';
            params['LONO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + LONO };
        }

        newSql += ` GROUP BY A.LONO, A.PRD_PC, A.LOT_NO, A.WEIGHT, A.INV_DATE, A.PPS_CODE, A.OPNO, B.LINE, B.SEQ, B.ACT_STR_TIME, C.PRO_SCHEDULE_LINE, C.PRO_SCHEDULE_SEQ, C.PACKING_DATE, C.PACKING_STATUS,
        MIN_SEQ_TABLE.LINE, MIN_SEQ_TABLE.MIN_SEQ, MIN_SEQ_TABLE_NEW.LINE, MIN_SEQ_TABLE_NEW.MIN_SEQ, MIN_SEQ_TABLE_3.LINE, MIN_SEQ_TABLE_3.SEQ, MIN_SEQ_TABLE_3_DIFF.LINE, MIN_SEQ_TABLE_3_DIFF.SEQ 
            ORDER BY PRO_SAMELINE NULLS LAST, PRO_DIFFERENTLINE NULLS LAST, NEXT_PRO NULLS LAST, NEXT_PRO_DIFF NULLS LAST `;

        const result = await conn.execute(newSql, params, { outFormat: oracledb.OBJECT });

        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryStock', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//格位盤點
export async function queryLonoStatus(user) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT LONO, STATUS
            FROM RM_LOC
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryLonoStatus', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//盤點格位內容
export async function queryLonoInfo(user, lono) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT PRD_PC, LOT_NO, WEIGHT, INV_DATE, OPNO 
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND STATUS = '1' `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lono },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryLonoInfo', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新格位內容
export async function updateLonoInfo(user, newProductNo, newLotNo, newWeight, opno, lono) {
    const obj = {
        res: [],
        error: '',
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const confirmPro = await confirmProSchedule(user, newProductNo, newLotNo);

        if (0 < confirmPro.res.length) {
            const RM_STGFLD_SQL = `
            UPDATE RM_STGFLD
                SET PRD_PC = :PRD_PC,
                    LOT_NO = :LOT_NO,
                    WEIGHT = :WEIGHT,
                    EDITOR = :EDITOR,
                    EDIT_DATE = SYSDATE
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND OPNO = :OPNO
                AND LONO = :LONO
                AND STATUS = '1' `;
            const RM_STGFLD_params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lono },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newProductNo },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newLotNo },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newWeight },
                EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            };

            const RM_BAGINFO_SQL = `
            INSERT INTO RM_BAGINFO(COMPANY, FIRM, DEPT, PRD_PC, LOT_NO, WEIGHT, OPNO, LONO, STATUS, CREATOR, REMARK) 
            VALUES( :COMPANY, :FIRM, :DEPT, :PRD_PC, :LOT_NO, :WEIGHT, :OPNO, :LONO, '3', :CREATOR, '更新內容' ) `;
            const RM_BAGINFO_params = {
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newProductNo },
                LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newLotNo },
                WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + newWeight },
                OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },
                LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lono },
                CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            };

            const STGResult = await conn.execute(RM_STGFLD_SQL, RM_STGFLD_params, { autoCommit: false });

            if (STGResult.rowsAffected) {
                const BAGResult = await conn.execute(RM_BAGINFO_SQL, RM_BAGINFO_params, { autoCommit: false });
                if (BAGResult.rowsAffected) {
                    await conn.commit();
                } else {
                    obj.error = '1';
                    throw new Error('格位資訊更新失敗!');
                }
            } else {
                obj.error = '1';
                throw new Error('格位資訊更新失敗!');
            }
        } else {
            obj.error = '2';
            throw new Error('規格或批號與主排程核對失敗!');
        }

    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'updateLonoInfo', err);
        // obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//刪除格位資料
export async function deleteLonoInfo(user, opno, lono, prd_pc, lot_no, weight, doingType) {
    const obj = {
        res: [],
        error: '',
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const STGSql = `
            UPDATE RM_STGFLD
                SET IO = 'O',
                    STATUS = '0',
                    PPS_CODE = :PPS_CODE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND OPNO = :OPNO
            AND IO = 'I' `;
        const STGparams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lono },
            OPNO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + opno },  //殘包標籤編號
        };

        const LOCSql = `
            UPDATE RM_LOC
                SET STATUS = '0',
                    LTIM = SYSDATE
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND LONO = :LONO
            AND STATUS = '1' `;
        const LOCParams = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            LONO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + lono },
        };

        const LOCResult = await conn.execute(LOCSql, LOCParams, { autoCommit: false });

        //先檢查格位狀態
        if (LOCResult.rowsAffected) {
            const STGResult = await conn.execute(STGSql, STGparams, { autoCommit: false });
            if (STGResult.rowsAffected) {
                await updateBagInfo(user, doingType, prd_pc, lot_no, weight, lono, opno);
                await conn.commit();
            } else {
                await errorRecord(user, 'OUT', lono, opno);
                throw new Error(`STG刪除格位異常，${lono}中的標籤${opno}錯誤`);
            }
        } else {
            errorRecord(user, 'OUT', lono, opno);
            throw new Error(`LOC刪除格位異常，${lono}格位無資料`);
        }
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'deleteLonoInfo', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//品番查格位
export async function getProductLono(user, productNo) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT PRD_PC, LOT_NO, INV_DATE, LONO, WEIGHT
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND STATUS = '1'
            AND IO = 'I'
            AND LONO IS NOT NULL
            AND PRD_PC = :PRD_PC
            ORDER BY INV_DATE `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getProductLono', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//查詢格位內存放過久的物件，每周一早上8點寄信通知
export async function alertStock(user, endDate) {
    const obj = {
        res: [],
        error: null,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT PRD_PC, LOT_NO, WEIGHT, LONO, INV_DATE, PPS_CODE
            FROM RM_STGFLD
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND STATUS = '1'
            AND TO_CHAR( INV_DATE, 'YYYYMMDD' ) <= :START_DATE
            ORDER BY INV_DATE `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        if (result.rows.length) {
            await Mailer.alertRemainBagStock(user, result.rows);
        }
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'alertStock', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

function getEveryTHUInMonth(date) {
    const daysInMonth = moment(date).daysInMonth() + 1;
    const firstDayOfMonth = moment(date).startOf('week');
    const result = [];

    for (let i = 0; i <= daysInMonth; i++) {
        let targetDate = moment(firstDayOfMonth).add(i, 'day');
        if ('Thursday' === moment(targetDate).format('dddd')) {
            const data = moment(targetDate).format('YYYYMMDD');
            result.push(data);
        }
        if (i === daysInMonth) {
            if ('Thursday' !== moment(targetDate).format('dddd')) {
                const data = moment(targetDate).format('YYYYMMDD');
                result.push(data);
            }
        }
    }
    return result;
}

//殘包管理報表(月報)
export async function queryBagMonthReport(user, startDate, endDate) {
    const obj = {
        res: [],
        error: null,
    };

    const startDateEndOfMonth = moment(startDate).endOf('month').format('YYYYMMDD');
    const startDateStartOfMonth = moment(startDate).startOf('month').format('YYYYMMDD');
    const endDateEndOfMonth = moment(endDate).endOf('month').format('YYYYMMDD');
    const endDateStartOfMonth = moment(endDate).startOf('month').format('YYYYMMDD');

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const remain_LastMonth = await getRemainOnStock(conn, user, startDateEndOfMonth);
        const remain_ThisMonth = await getRemainOnStock(conn, user, endDateEndOfMonth);

        const totalOutput_LastMonth = await getRemainReason(conn, user, startDateStartOfMonth, startDateEndOfMonth);
        const totalOutput_ThisMonth = await getRemainReason(conn, user, endDateStartOfMonth, endDateEndOfMonth);

        const reason1_LastMonth = await getReason1(conn, user, startDateStartOfMonth, startDateEndOfMonth, 'month');
        const reason1_ThisMonth = await getReason1(conn, user, endDateStartOfMonth, endDateEndOfMonth, 'month');
        const reason2_LastMonth = await getReason2(conn, user, startDateStartOfMonth, startDateEndOfMonth, 'month');
        const reason2_ThisMonth = await getReason2(conn, user, endDateStartOfMonth, endDateEndOfMonth, 'month');
        const reason3_LastMonth = await getReason3(conn, user, startDateStartOfMonth, startDateEndOfMonth, 'month');
        const reason3_ThisMonth = await getReason3(conn, user, endDateStartOfMonth, endDateEndOfMonth, 'month');
        const output_LastMonth = await getOutput(conn, user, startDateStartOfMonth, startDateEndOfMonth, 'month');
        const output_ThisMonth = await getOutput(conn, user, endDateStartOfMonth, endDateEndOfMonth, 'month');

        const total_COUNT_LastMonth = totalOutput_LastMonth.map(item => {
            return {
                LABEL_1: '合計處理',
                LABEL_2: '包數',
                TOTAL_REASON: item.TOTAL_REASON,
            };
        });
        const total_WEIGHT_LastMonth = totalOutput_LastMonth.map(item => {
            return {
                LABEL_1: '合計處理',
                LABEL_2: '重量',
                TOTAL_WEIGHT: item.TOTAL_WEIGHT,
            };
        });
        const reason1_COUNT_LastMonth = reason1_LastMonth.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '包數',
                COUNT: item.COUNT_REASON_PRO,
            };
        });
        const reason1_WEIGHT_LastMonth = reason1_LastMonth.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '重量',
                REASON_PRO_WEIGHT: item.REASON_PRO_WEIGHT,
            };
        });
        const reason2_COUNT_LastMonth = reason2_LastMonth.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '包數',
                COUNT_REASON_PACKING: item.COUNT_REASON_PACKING,
            };
        });
        const reason2_WEIGHT_LastMonth = reason2_LastMonth.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '重量',
                REASON_PACKING_WEIGHT: item.REASON_PACKING_WEIGHT,
            };
        });
        const reason3_COUNT_LastMonth = reason3_LastMonth.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '包數',
                COUNT_REASON_3: item.COUNT_REASON_3,
            };
        });
        const reason3_WEIGHT_LastMonth = reason3_LastMonth.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '重量',
                REASON_3_WEIGHT: item.REASON_3_WEIGHT,
            };
        });
        const output_COUNT_LastMonth = output_LastMonth.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '包數',
                COUNT_INSTORAGE: item.COUNT_INSTORAGE,
            };
        });
        const output_WEIGHT_LastMonth = output_LastMonth.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '重量',
                WEIGHT_INSTORAGE: item.INSTORAGE_WEIGHT,
            };
        });
        const remain_COUNT_LastMonth = remain_LastMonth.map(item => {
            return {
                LABEL_1: '殘包庫存',
                LABEL_2: '包數',
                INSTOCK_COUNT: item.INSTOCK_COUNT,
            };
        });
        const remain_WEIGHT_LastMonth = remain_LastMonth.map(item => {
            return {
                LABEL_1: '殘包庫存',
                LABEL_2: '重量',
                INSTOCK_WEIGHT: item.INSTOCK_WEIGHT,
            };
        });

        const total_COUNT_ThisMonth = totalOutput_ThisMonth.map(item => {
            return {
                LABEL_1: '合計處理',
                LABEL_2: '包數',
                TOTAL_REASON_ThisMonth: item.TOTAL_REASON,
            };
        });
        const total_WEIGHT_ThisMonth = totalOutput_ThisMonth.map(item => {
            return {
                LABEL_1: '合計處理',
                LABEL_2: '重量',
                TOTAL_WEIGHT_ThisMonth: item.TOTAL_WEIGHT,
            };
        });
        const reason1_COUNT_ThisMonth = reason1_ThisMonth.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '包數',
                COUNT_ThisMonth: item.COUNT_REASON_PRO,
            };
        });
        const reason1_WEIGHT_ThisMonth = reason1_ThisMonth.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '重量',
                REASON_PRO_WEIGHT_ThisMonth: item.REASON_PRO_WEIGHT,
            };
        });
        const reason2_COUNT_ThisMonth = reason2_ThisMonth.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '包數',
                COUNT_REASON_PACKING_ThisMonth: item.COUNT_REASON_PACKING,
            };
        });
        const reason2_WEIGHT_ThisMonth = reason2_ThisMonth.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '重量',
                REASON_PACKING_WEIGHT_ThisMonth: item.REASON_PACKING_WEIGHT,
            };
        });
        const reason3_COUNT_ThisMonth = reason3_ThisMonth.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '包數',
                COUNT_REASON_3_ThisMonth: item.COUNT_REASON_3,
            };
        });
        const reason3_WEIGHT_ThisMonth = reason3_ThisMonth.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '重量',
                REASON_3_WEIGHT_ThisMonth: item.REASON_3_WEIGHT,
            };
        });
        const output_COUNT_ThisMonth = output_ThisMonth.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '包數',
                COUNT_INSTORAGE_ThisMonth: item.COUNT_INSTORAGE,
            };
        });
        const output_WEIGHT_ThisMonth = output_ThisMonth.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '重量',
                WEIGHT_INSTORAGE_ThisMonth: item.INSTORAGE_WEIGHT,
            };
        });
        const remain_COUNT_ThisMonth = remain_ThisMonth.map(item => {
            return {
                LABEL_1: '殘包庫存',
                LABEL_2: '包數',
                INSTOCK_COUNT_ThisMonth: item.INSTOCK_COUNT,
            };
        });
        const remain_WEIGHT_ThisMonth = remain_ThisMonth.map(item => {
            return {
                LABEL_1: '殘包庫存',
                LABEL_2: '重量',
                INSTOCK_WEIGHT_ThisMonth: item.INSTOCK_WEIGHT,
            };
        });

        const total_COUNT_Result = [...total_COUNT_LastMonth, ...total_COUNT_ThisMonth];
        const total_WEIGHT_Result = [...total_WEIGHT_LastMonth, ...total_WEIGHT_ThisMonth];
        const reason1_COUNT_Result = [...reason1_COUNT_LastMonth, ...reason1_COUNT_ThisMonth];
        const reason1_WEIGHT_Result = [...reason1_WEIGHT_LastMonth, ...reason1_WEIGHT_ThisMonth];
        const reason2_COUNT_Result = [...reason2_COUNT_LastMonth, ...reason2_COUNT_ThisMonth];
        const reason2_WEIGHT_Result = [...reason2_WEIGHT_LastMonth, ...reason2_WEIGHT_ThisMonth];
        const reason3_COUNT_Result = [...reason3_COUNT_LastMonth, ...reason3_COUNT_ThisMonth];
        const reason3_WEIGHT_Result = [...reason3_WEIGHT_LastMonth, ...reason3_WEIGHT_ThisMonth];
        const output_COUNT_Result = [...output_COUNT_LastMonth, ...output_COUNT_ThisMonth];
        const output_WEIGHT_Result = [...output_WEIGHT_LastMonth, ...output_WEIGHT_ThisMonth];
        const remain_COUNT_Result = [...remain_COUNT_LastMonth, ...remain_COUNT_ThisMonth];
        const remain_WEIGHT_Result = [...remain_WEIGHT_LastMonth, ...remain_WEIGHT_ThisMonth];

        const totalCountData = {};
        const reason1CountData = {};
        const reason1WeightData = {};
        const reason2CountData = {};
        const reason2WeightData = {};
        const reason3CountData = {};
        const reason3WeightData = {};
        const outputCountData = {};
        const outputWeightData = {};
        const remainCountData = {};
        const remainWeightData = {};

        total_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!totalCountData[label] && '包數' === label_2) {
                totalCountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof totalCountData[label]) {
                totalCountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                totalCountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        const totalWeightData = {};
        total_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!totalWeightData[label] && '重量' === label_2) {
                totalWeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof totalWeightData[label]) {
                totalWeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                totalWeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });
        reason1_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason1CountData[label] && '包數' === label_2) {
                reason1CountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason1CountData[label]) {
                reason1CountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                reason1CountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        reason1_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason1WeightData[label] && '重量' === label_2) {
                reason1WeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason1WeightData[label]) {
                reason1WeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                reason1WeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });
        reason2_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason2CountData[label] && '包數' === label_2) {
                reason2CountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason2CountData[label]) {
                reason2CountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                reason2CountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        reason2_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason2WeightData[label] && '重量' === label_2) {
                reason2WeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason2WeightData[label]) {
                reason2WeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                reason2WeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });
        reason3_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason3CountData[label] && '包數' === label_2) {
                reason3CountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason3CountData[label]) {
                reason3CountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                reason3CountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        reason3_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!reason3WeightData[label] && '重量' === label_2) {
                reason3WeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof reason3WeightData[label]) {
                reason3WeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                reason3WeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });
        output_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!outputCountData[label] && '包數' === label_2) {
                outputCountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof outputCountData[label]) {
                outputCountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                outputCountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        output_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!outputWeightData[label] && '重量' === label_2) {
                outputWeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof outputWeightData[label]) {
                outputWeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                outputWeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });
        remain_COUNT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!remainCountData[label] && '包數' === label_2) {
                remainCountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof remainCountData[label]) {
                remainCountData[label].LAST_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                remainCountData[label].THIS_COUNT += item.COUNT_ThisMonth || item.COUNT_REASON_PACKING_ThisMonth || item.COUNT_REASON_3_ThisMonth || item.COUNT_INSTORAGE_ThisMonth || item.INSTOCK_COUNT_ThisMonth || item.TOTAL_REASON_ThisMonth || 0;
            }
        });
        remain_WEIGHT_Result.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;
            if (!remainWeightData[label] && '重量' === label_2) {
                remainWeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    LAST_COUNT: 0,
                    THIS_COUNT: 0,
                };
            }
            if ('undefined' !== typeof remainWeightData[label]) {
                remainWeightData[label].LAST_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                remainWeightData[label].THIS_COUNT += item.REASON_PRO_WEIGHT_ThisMonth || item.REASON_PACKING_WEIGHT_ThisMonth || item.REASON_3_WEIGHT_ThisMonth || item.INSTOCK_WEIGHT_ThisMonth || item.WEIGHT_INSTORAGE_ThisMonth || item.TOTAL_WEIGHT_ThisMonth || 0;
            }
        });

        const totalCountResult = Object.values(totalCountData);
        const totalWeightResult = Object.values(totalWeightData);
        const reason1CountResult = Object.values(reason1CountData);
        const reason1WeightResult = Object.values(reason1WeightData);
        const reason2CountResult = Object.values(reason2CountData);
        const reason2WeightResult = Object.values(reason2WeightData);
        const reason3CountResult = Object.values(reason3CountData);
        const reason3WeightResult = Object.values(reason3WeightData);
        const outputCountResult = Object.values(outputCountData);
        const outputWeightResult = Object.values(outputWeightData);
        const remainCountResult = Object.values(remainCountData);
        const remainWeightResult = Object.values(remainWeightData);

        const merged = [...totalCountResult, ...totalWeightResult, ...reason1CountResult, ...reason1WeightResult, ...reason2CountResult, ...reason2WeightResult,
            ...reason3CountResult, ...reason3WeightResult, ...outputCountResult, ...outputWeightResult, ...remainCountResult, ...remainWeightResult];

        const orderArray = ['殘包產出', '押出回摻', '包裝回摻', '重工去化', '合計處理', '殘包庫存'];

        merged.sort((a, b) => orderArray.indexOf(a.LABEL_1) - orderArray.indexOf(b.LABEL_1));

        obj.res = merged;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryBagMonthReport', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//殘包管理報表(週報)
export async function queryBagWeekReport(user, startDate, endDate) {
    const obj = {
        res: [],
        err: null,
    };

    const date = moment(startDate).format('YYYY-MM');

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const everyThu = getEveryTHUInMonth(date);
        const onStockResult = [];

        for (const item of everyThu) {
            onStockResult.push(await getRemainOnStock(conn, user, item));
        }
        const totalOutput = await getRemainReason(conn, user, startDate, endDate, 'week');

        const reason1 = await getReason1(conn, user, startDate, endDate, 'week');
        const reason2 = await getReason2(conn, user, startDate, endDate, 'week');
        const reason3 = await getReason3(conn, user, startDate, endDate, 'week');
        const output = await getOutput(conn, user, startDate, endDate, 'week');

        totalOutput.sort((a, b) => a.WEEK_PER_MONTH - b.WEEK_PER_MONTH);
        //合計處理(包數)
        const total_COUNT = totalOutput.map(item => {
            return {
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                LABEL_1: '合計處理',
                LABEL_2: '包數',
                TOTAL_REASON: item.TOTAL_REASON,
            };
        });
        //合計處理(重量)
        const total_WEIGHT = totalOutput.map(item => {
            return {
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                LABEL_1: '合計處理',
                LABEL_2: '重量',
                TOTAL_WEIGHT: item.TOTAL_WEIGHT,
            };
        });
        //殘包庫存(包數)
        const totalOutput_COUNT = totalOutput.map((item, index) => {
            const instockItem = onStockResult[index][0];
            return {
                INSTOCK_COUNT: instockItem.INSTOCK_COUNT,
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                LABEL_1: '殘包庫存',
                LABEL_2: '包數',
            };
        });
        //殘包庫存(重量)
        const totalOutput_WEIGHT = totalOutput.map((item, index) => {
            const instockItem = onStockResult[index][0];
            return {
                INSTOCK_WEIGHT: instockItem.INSTOCK_WEIGHT,
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                LABEL_1: '殘包庫存',
                LABEL_2: '重量',
            };
        });
        //押出回摻(包數)
        const reason1_COUNT = reason1.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '包數',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                COUNT: item.COUNT_REASON_PRO,
            };
        });
        //押出回摻(重量)
        const reason1_WEIGHT = reason1.map(item => {
            return {
                LABEL_1: '押出回摻',
                LABEL_2: '重量',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                REASON_PRO_WEIGHT: item.REASON_PRO_WEIGHT,
            };
        });
        //包裝回摻(包數)
        const reason2_COUNT = reason2.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '包數',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                COUNT_REASON_PACKING: item.COUNT_REASON_PACKING,
            };
        });
        //包裝回摻(重量)
        const reason2_WEIGHT = reason2.map(item => {
            return {
                LABEL_1: '包裝回摻',
                LABEL_2: '重量',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                REASON_PACKING_WEIGHT: item.REASON_PACKING_WEIGHT,
            };
        });
        //重工去化(包數)
        const reason3_COUNT = reason3.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '包數',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                COUNT_REASON_3: item.COUNT_REASON_3,
            };
        });
        //重工去化(重量)
        const reason3_WEIGHT = reason3.map(item => {
            return {
                LABEL_1: '重工去化',
                LABEL_2: '重量',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                REASON_3_WEIGHT: item.REASON_3_WEIGHT,
            };
        });
        //殘包產出(包數)
        const output_COUNT = output.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '包數',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                COUNT_INSTORAGE: item.COUNT_INSTORAGE,
            };
        });
        //殘包產出(重量)
        const output_WEIGHT = output.map(item => {
            return {
                LABEL_1: '殘包產出',
                LABEL_2: '重量',
                WEEK_PER_MONTH: item.WEEK_PER_MONTH,
                WEIGHT_INSTORAGE: item.INSTORAGE_WEIGHT,
            };
        });

        const result_COUNT = [...output_COUNT, ...reason3_COUNT, ...reason2_COUNT, ...reason1_COUNT, ...totalOutput_COUNT, ...total_COUNT];
        const result_WEIGHT = [...output_WEIGHT, ...reason3_WEIGHT, ...reason2_WEIGHT, ...reason1_WEIGHT, ...totalOutput_WEIGHT, ...total_WEIGHT];

        const integratedCountData = {};

        result_COUNT.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;

            if (!integratedCountData[label] && '包數' === label_2) {
                integratedCountData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    W1_COUNT: 0,
                    W2_COUNT: 0,
                    W3_COUNT: 0,
                    W4_COUNT: 0,
                    W5_COUNT: 0,
                    TOTAL: '',
                };
            }
            if ('undefined' !== typeof integratedCountData[label]) {
                if (1 === item.WEEK_PER_MONTH) {
                    integratedCountData[label].W1_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                } else if (2 === item.WEEK_PER_MONTH) {
                    integratedCountData[label].W2_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                } else if (3 === item.WEEK_PER_MONTH) {
                    integratedCountData[label].W3_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                } else if (4 === item.WEEK_PER_MONTH) {
                    integratedCountData[label].W4_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                } else if (5 === item.WEEK_PER_MONTH) {
                    integratedCountData[label].W5_COUNT += item.COUNT || item.COUNT_REASON_PACKING || item.COUNT_REASON_3 || item.COUNT_INSTORAGE || item.INSTOCK_COUNT || item.TOTAL_REASON || 0;
                }
                if ('殘包產出' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '=SUM(C1:G1)';
                } else if ('押出回摻' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '=SUM(C3:G3)';
                } else if ('包裝回摻' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '=SUM(C5:G5)';
                } else if ('重工去化' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '=SUM(C7:G7)';
                } else if ('合計處理' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '=SUM(C9:G9)';
                } else if ('殘包庫存' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedCountData[label].TOTAL += '';
                }
            }
        });

        const integratedWeightData = {};

        result_WEIGHT.forEach(item => {
            const label = item.LABEL_1;
            const label_2 = item.LABEL_2;

            if (!integratedWeightData[label] && '重量' === label_2) {
                integratedWeightData[label] = {
                    LABEL_1: label,
                    LABEL_2: item.LABEL_2,
                    W1_COUNT: 0,
                    W2_COUNT: 0,
                    W3_COUNT: 0,
                    W4_COUNT: 0,
                    W5_COUNT: 0,
                    TOTAL: '',
                };
            }
            if ('undefined' !== typeof integratedWeightData) {
                if (1 === item.WEEK_PER_MONTH) {
                    integratedWeightData[label].W1_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                } else if (2 === item.WEEK_PER_MONTH) {
                    integratedWeightData[label].W2_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                } else if (3 === item.WEEK_PER_MONTH) {
                    integratedWeightData[label].W3_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                } else if (4 === item.WEEK_PER_MONTH) {
                    integratedWeightData[label].W4_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                } else if (5 === item.WEEK_PER_MONTH) {
                    integratedWeightData[label].W5_COUNT += item.REASON_PRO_WEIGHT || item.REASON_PACKING_WEIGHT || item.REASON_3_WEIGHT || item.INSTOCK_WEIGHT || item.WEIGHT_INSTORAGE || item.TOTAL_WEIGHT || 0;
                }
                if ('殘包產出' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '=SUM(C2:G2)';
                } else if ('押出回摻' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '=SUM(C4:G4)';
                } else if ('包裝回摻' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '=SUM(C6:G6)';
                } else if ('重工去化' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '=SUM(C8:G8)';
                } else if ('合計處理' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '=SUM(C10:G10)';
                } else if ('殘包庫存' === label /*&& 5 === item.WEEK_PER_MONTH*/) {
                    integratedWeightData[label].TOTAL += '';
                }
            }
        });

        const countResult = Object.values(integratedCountData);
        const weightResult = Object.values(integratedWeightData);

        const merged = [...countResult, ...weightResult];

        const orderArray = ['殘包產出', '押出回摻', '包裝回摻', '重工去化', '合計處理', '殘包庫存'];

        merged.sort((a, b) => orderArray.indexOf(a.LABEL_1) - orderArray.indexOf(b.LABEL_1));

        obj.res = merged;

    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'queryBagReport_NEW', err);
        obj.err = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//殘包庫存
async function getRemainOnStock(conn, user, endDate, type) {

    let sql;
    let params;
    if ('week' === type) {
        sql = `
        SELECT 
            COUNT(CASE WHEN STATUS = '1' THEN 1 ELSE NULL END) AS INSTOCK_COUNT,
            SUM(CASE WHEN STATUS = '1' THEN WEIGHT ELSE NULL END) AS INSTOCK_WEIGHT
        FROM RM_STGFLD
        WHERE 
            TO_CHAR(INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    sql = `
        SELECT 
            COUNT(CASE WHEN STATUS = '1' THEN 1 ELSE NULL END) AS INSTOCK_COUNT,
            SUM(CASE WHEN STATUS = '1' THEN WEIGHT ELSE NULL END) AS INSTOCK_WEIGHT
        FROM RM_STGFLD
        WHERE 
            TO_CHAR(INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT `;
    params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;

}

//合計處理
async function getRemainReason(conn, user, startDate, endDate, type) {

    if ('week' === type) {
        const sql = `
        SELECT 
            WEEK_PER_MONTH,
            SUM(COUNT_REASON_1 + COUNT_REASON_2 + COUNT_REASON_3) AS TOTAL_REASON,
            SUM(REASON_1_WEIGHT + REASON_2_WEIGHT + REASON_3_WEIGHT) AS TOTAL_WEIGHT,
            COUNT_REASON_1, REASON_1_WEIGHT, COUNT_REASON_2, REASON_2_WEIGHT,
            COUNT_REASON_3, REASON_3_WEIGHT, COUNT_INSTORAGE, INSTORAGE_WEIGHT
        FROM (
            SELECT 
                FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1 AS WEEK_PER_MONTH,
                COUNT(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN 1 ELSE NULL END) AS COUNT_REASON_1,
                SUM(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN A.WEIGHT ELSE 0 END) AS REASON_1_WEIGHT,
                COUNT(CASE WHEN A.REASON = '包裝回摻' THEN 1 ELSE NULL END) AS COUNT_REASON_2,
                SUM(CASE WHEN A.REASON = '包裝回摻' AND A.STATUS = '0' THEN A.WEIGHT ELSE 0 END) AS REASON_2_WEIGHT, 
                COUNT(CASE WHEN A.REASON = '重工去化' THEN 1 ELSE NULL END) AS COUNT_REASON_3,
                SUM(CASE WHEN A.REASON = '重工去化' AND A.STATUS = '0' THEN A.WEIGHT ELSE 0 END) AS REASON_3_WEIGHT,
                COUNT(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND A.REMARK IS NULL THEN 1 ELSE NULL END) AS COUNT_INSTORAGE,
                SUM(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND REMARK IS NULL THEN A.WEIGHT ELSE 0 END) AS INSTORAGE_WEIGHT
            FROM 
                RM_BAGINFO A
            WHERE 
                TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
                AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
                AND A.COMPANY = :COMPANY
                AND A.FIRM = :FIRM
                AND A.DEPT = :DEPT
            GROUP BY 
                FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1
        ) SUBQUERY
        GROUP BY WEEK_PER_MONTH, COUNT_REASON_1, REASON_1_WEIGHT, COUNT_REASON_2, REASON_2_WEIGHT,
            COUNT_REASON_3, REASON_3_WEIGHT, COUNT_INSTORAGE, INSTORAGE_WEIGHT `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    const sql = `
    SELECT 
        SUM(COUNT_REASON_1 + COUNT_REASON_2 + COUNT_REASON_3) AS TOTAL_REASON,
        SUM(REASON_1_WEIGHT + REASON_2_WEIGHT + REASON_3_WEIGHT) AS TOTAL_WEIGHT,
        COUNT_REASON_1, REASON_1_WEIGHT, COUNT_REASON_2, REASON_2_WEIGHT,
        COUNT_REASON_3, REASON_3_WEIGHT, COUNT_INSTORAGE, INSTORAGE_WEIGHT
    FROM (
        SELECT 
            COUNT(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN 1 ELSE NULL END) AS COUNT_REASON_1,
            SUM(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN A.WEIGHT ELSE 0 END) AS REASON_1_WEIGHT,
            COUNT(CASE WHEN A.REASON = '包裝回摻' THEN 1 ELSE NULL END) AS COUNT_REASON_2,
            SUM(CASE WHEN A.REASON = '包裝回摻' AND A.STATUS = '0' THEN A.WEIGHT ELSE 0 END) AS REASON_2_WEIGHT, 
            COUNT(CASE WHEN A.REASON = '重工去化' THEN 1 ELSE NULL END) AS COUNT_REASON_3,
            SUM(CASE WHEN A.REASON = '重工去化' AND A.STATUS = '0' THEN A.WEIGHT ELSE 0 END) AS REASON_3_WEIGHT,
            COUNT(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND A.REMARK IS NULL THEN 1 ELSE NULL END) AS COUNT_INSTORAGE,
            SUM(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND REMARK IS NULL THEN A.WEIGHT ELSE 0 END) AS INSTORAGE_WEIGHT
        FROM 
            RM_BAGINFO A
        WHERE 
            TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
    ) SUBQUERY
    GROUP BY COUNT_REASON_1, REASON_1_WEIGHT, COUNT_REASON_2, REASON_2_WEIGHT,
        COUNT_REASON_3, REASON_3_WEIGHT, COUNT_INSTORAGE, INSTORAGE_WEIGHT `;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;
}

//押出回摻
async function getReason1(conn, user, startDate, endDate, type) {
    if ('week' == type) {
        const sql = `
        SELECT FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1 AS WEEK_PER_MONTH, 
            COUNT(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN 1 ELSE NULL END) AS COUNT_REASON_PRO,
            SUM(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN A.WEIGHT ELSE 0 END) AS REASON_PRO_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
        GROUP BY 
            FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1  `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    const sql = `
        SELECT  
            COUNT(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN 1 ELSE NULL END) AS COUNT_REASON_PRO,
            SUM(CASE WHEN (A.REASON = '押出回摻' AND A.STATUS = '0') OR (A.REASON IS NULL AND A.STATUS = '0') THEN A.WEIGHT ELSE 0 END) AS REASON_PRO_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT `;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;
}

//包裝回摻
async function getReason2(conn, user, startDate, endDate, type) {
    if ('week' === type) {
        const sql = `
        SELECT FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1 AS WEEK_PER_MONTH, 
            COUNT(CASE WHEN A.REASON = '包裝回摻' THEN 1 ELSE NULL END) AS COUNT_REASON_PACKING,
            SUM(CASE WHEN A.REASON = '包裝回摻' THEN A.WEIGHT ELSE 0 END) AS REASON_PACKING_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
        GROUP BY 
            FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1  `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    const sql = `
        SELECT 
            COUNT(CASE WHEN A.REASON = '包裝回摻' THEN 1 ELSE NULL END) AS COUNT_REASON_PACKING,
            SUM(CASE WHEN A.REASON = '包裝回摻' THEN A.WEIGHT ELSE 0 END) AS REASON_PACKING_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT `;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;
}

//重工去化
async function getReason3(conn, user, startDate, endDate, type) {
    if ('week' === type) {
        const sql = `
        SELECT FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1 AS WEEK_PER_MONTH, 
            COUNT(CASE WHEN A.REASON = '重工去化' THEN 1 ELSE NULL END) AS COUNT_REASON_3,
            SUM(CASE WHEN A.REASON = '重工去化' THEN A.WEIGHT ELSE 0 END) AS REASON_3_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
        GROUP BY 
            FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1  `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    const sql = `
        SELECT 
            COUNT(CASE WHEN A.REASON = '重工去化' THEN 1 ELSE NULL END) AS COUNT_REASON_3,
            SUM(CASE WHEN A.REASON = '重工去化' THEN A.WEIGHT ELSE 0 END) AS REASON_3_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT `;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;
}

//殘包產出
async function getOutput(conn, user, startDate, endDate, type) {
    if ('week' === type) {
        const sql = `
        SELECT FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1 AS WEEK_PER_MONTH, 
            COUNT(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL THEN 1 ELSE NULL END) AS COUNT_INSTORAGE,
            SUM(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL THEN A.WEIGHT ELSE 0 END) AS INSTORAGE_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT
        GROUP BY 
            FLOOR((EXTRACT(DAY FROM A.INV_DATE) + 5 - MOD(TO_CHAR(A.INV_DATE, 'D') + 1, 7)) / 7) + 1  `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
            END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
        };

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        return result.rows;
    }
    const sql = `
        SELECT 
            COUNT(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND A.REMARK IS NULL THEN 1 ELSE NULL END) AS COUNT_INSTORAGE,
            SUM(CASE WHEN A.STATUS = '1' AND A.REASON IS NULL AND REMARK IS NULL THEN A.WEIGHT ELSE 0 END) AS INSTORAGE_WEIGHT 
        FROM RM_BAGINFO A
        WHERE TO_CHAR(A.INV_DATE, 'YYYYMMDD') >= :START_DATE
            AND TO_CHAR(A.INV_DATE, 'YYYYMMDD') <= :END_DATE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            AND A.DEPT = :DEPT `;
    const params = {
        COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
        FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        START_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + startDate },
        END_DATE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + endDate },
    };

    const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

    return result.rows;
}

export async function exportExcel(data, type, thisDate, lastDate) {
    const inputExcelPath_week = './src/packing/report/殘包管理報表_週報.xlsx';
    const inputExcelPath_month = './src/packing/report/殘包管理報表_月報.xlsx';
    const workbook = new ExcelJS.Workbook();
    if (1 === type) {
        await workbook.xlsx.readFile(inputExcelPath_week);

        const weeklyReport = workbook.getWorksheet('週報');
        const insertRow_week = ['W1_COUNT', 'W2_COUNT', 'W3_COUNT', 'W4_COUNT', 'W5_COUNT', ];
        weeklyReport.insertRow(4, insertRow_week);
        weeklyReport.getRow(4).hidden = true;
        const targetData_week = weeklyReport.getRow(4).values;

        const weeklyData = {};

        let startRow = 5;
        let startCol = 'E';

        const titleCell = weeklyReport.getCell('E2');
        titleCell.value = `${thisDate}月`;

        for (let i = 0; i < data.length; i++) {
            targetData_week.forEach(header => {
                weeklyData[header] = data[i][header];
                const cell = weeklyReport.getCell(`${startCol}${startRow}`);
                const col = weeklyReport.getColumn(`${startCol}`);
                col.width = 20;
                cell.value = weeklyData[header];
                startCol = String.fromCharCode(startCol.charCodeAt(0) + 2);
            });
            startCol = 'E';
            startRow += 1;
        }

        const buffer = await workbook.xlsx.writeBuffer();

        return buffer;
    }
    await workbook.xlsx.readFile(inputExcelPath_month);

    const monthlyReport = workbook.getWorksheet('月報');
    const insertRow_month = ['LAST_COUNT', 'THIS_COUNT'];
    monthlyReport.insertRow(4, insertRow_month);
    monthlyReport.getRow(4).hidden = true;
    const targetData_month = monthlyReport.getRow(4).values;

    const monthlyData = {};
    let startRow = 5;
    let startCol = 'E';

    const titleCell_last = monthlyReport.getCell('E2');
    const titleCell_this = monthlyReport.getCell('G2');
    titleCell_last.value = `${lastDate}月`;
    titleCell_this.value = `${thisDate}月`;

    for (let i = 0; i < data.length; i++) {
        targetData_month.forEach(header => {
            monthlyData[header] = data[i][header];
            const cell = monthlyReport.getCell(`${startCol}${startRow}`);
            const col = monthlyReport.getColumn(`${startCol}`);
            col.width = 20;
            cell.value = monthlyData[header];
            startCol = String.fromCharCode(startCol.charCodeAt(0) + 2); //有合併儲存格，所以下一格+2
        });
        startCol = 'E';
        startRow += 1;
    }

    const buffer = await workbook.xlsx.writeBuffer();

    return buffer;

}
import config from './config.js';
import { getNowDatetimeString } from './libs.js';
import oracledb from 'oracledb';

//取得入料機
export async function getFeeder(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT LINE, FEEDER
            FROM PBTC_IOT_FEEDER_INFO
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFeeder Error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
//檔案維護
export async function getFeederFileMaintain(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT COMPANY, FIRM, LINE, FEEDER, TOLERANCE_RATIO
            FROM PBTC_IOT_FEEDER_INFO
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM 
            ORDER BY LINE, FEEDER `; //依照兩個條件排序
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFeederFileMaintain Error', err);
        obj.res = '' + err;
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function createFeeder(line, feeder, tolerance, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_FEEDER_INFO 
            ( COMPANY, FIRM, LINE, FEEDER, EDITOR, TOLERANCE_RATIO)
            VALUES
            ( :COMPANY, :FIRM, :LINE, :FEEDER, :EDITOR, :TOLERANCE_RATIO) `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            FEEDER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feeder },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            TOLERANCE_RATIO: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(tolerance) },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'createFeeder Error', err);
        obj.res = '' + err;
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function deleteFeeder(line, feeder, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            DELETE PBTC_IOT_FEEDER_INFO
            WHERE FEEDER = :FEEDER
            AND LINE = :LINE
            AND COMPANY = :COMPANY 
            AND FIRM = :FIRM `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            FEEDER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feeder },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'deleteFeeder Error', err);
        obj.res = '' + err;
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function updateFeeder(feederArray, user) {
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
            UPDATE PBTC_IOT_FEEDER_INFO
            SET TOLERANCE_RATIO = :TOLERANCE_RATIO 
            WHERE LINE = :LINE
            AND FIRM = :FIRM
            AND COMPANY = :COMPANY
            AND FEEDER = :FEEDER `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feederArray.LINE },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FEEDER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + feederArray.FEEDER },
            TOLERANCE_RATIO: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(feederArray.TOLERANCE_RATIO) },
        };
        const commit = { autoCommit: true };
        await conn.execute(sql, params, commit);
    } catch (error) {
        console.error(getNowDatetimeString(), 'updateFeeder Error', error);
        obj.res = '' + error;
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
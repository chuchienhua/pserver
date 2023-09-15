import config from './config.js';
import { getNowDatetimeString } from './libs.js';
import oracledb from 'oracledb';

//帳號權限部分
export async function getAllUserAuth(userPPS, firm) {
    let conn;
    let rows = [];
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT S0.ROUTE, S0.ISADMIN, S1.ROUTE_NAME, S1.IS_HIDE
            FROM PBTC_IOT_AUTH S0 LEFT JOIN PBTC_IOT_ROUTE_SETTINGS S1
                ON S0.ROUTE = S1.ROUTE
            WHERE S0.PPS_CODE = :PPS_CODE
            AND S0.FIRM = :FIRM
            ORDER BY S1.ROUTE_ORDER `;
        const params = {
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + userPPS },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + firm },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        rows = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getAllUserAuth', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }
    // console.log('rows', rows);
    return rows;
}

//取得所有廠擁有功能的員工編號
export async function getFirmUser() {
    let conn;
    let rows = [];
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT FIRM, PPS_CODE
            FROM PBTC_IOT_AUTH
            GROUP BY FIRM, PPS_CODE `;
        const result = await conn.execute(sql, {}, { outFormat: oracledb.OBJECT });
        rows = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFirmUser', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return rows;
}

//取得所有可用的Routes
export async function getAllRoutes(user) {
    let conn;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT S0.PPS_CODE, S0.ROUTE, S0.ISADMIN, S1.NAME, S2.ROUTE_NAME
            FROM PBTC_IOT_AUTH S0 
                LEFT JOIN PERSON_FULL S1
                    ON S0.PPS_CODE = S1.PPS_CODE
                    AND S1.IS_ACTIVE IN ('A', 'T')
                LEFT JOIN PBTC_IOT_ROUTE_SETTINGS S2
                    ON S0.ROUTE = S2.ROUTE
            WHERE S0.ROUTE != 'authManage'
            AND S0.ROUTE != 'recipe'
            AND S0.COMPANY = :COMPANY
            AND S0.FIRM = :FIRM
            ORDER BY S2.ROUTE_ORDER, S0.PPS_CODE  `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
    } catch (err) {
        console.error(getNowDatetimeString(), 'getAllRoutes', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return result.rows;
}

//新增使用者權限
export async function addRouteUser(ppsCode, route, isAdmin, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            INSERT INTO PBTC_IOT_AUTH ( PPS_CODE, ROUTE, ISADMIN, EDITOR, COMPANY, FIRM )
            VALUES ( :PPS_CODE, :ROUTE, ${(isAdmin) ? '1' : '0'} , :EDITOR, :COMPANY, :FIRM )`;
        const params = {
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ppsCode.toString() },
            ROUTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: route.toString() },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'addRouteUser', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}


//移除使用者權限
export async function removeRouteUser(ppsCode, route, isAdmin, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            DELETE PBTC_IOT_AUTH
            WHERE PPS_CODE = :PPS_CODE
            AND ROUTE = :ROUTE
            AND ISADMIN = ${(isAdmin) ? '1' : '0'} 
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: ppsCode.toString() },
            ROUTE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: route.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let result = await conn.execute(sql, params, { autoCommit: true });
        if (!result.rowsAffected) {
            throw new Error(`ROUTE: ${route}; 為找到使用者: ${ppsCode}`);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeRouteUser', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//員工編號查詢名字、所屬公司廠別
export async function getUserData(ppsCode) {
    let obj = {
        res: [],
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const sql = `
            SELECT *
            FROM PERSON_FULL
            WHERE PPS_CODE = :PPS_CODE
            AND IS_ACTIVE IN ('A', 'T')`;
        const params = {
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + ppsCode },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getUserData', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
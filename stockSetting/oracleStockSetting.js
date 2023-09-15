import config from '../config.js';
import * as libs from '../libs.js';
import oracledb from 'oracledb';

export async function getStock(user, targetName, searchType) {
    const obj = {
        res: [],
        error: '',
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            SELECT NAME, SAFETY_STOCK, STOCK_MAX
            FROM PBTC_IOT_STOCK_SETTING
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND DEPT = :DEPT
            AND PM = :PM `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            PM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + searchType },
        };

        if ('*' !== targetName) {
            sql += ' AND NAME = :NAME ';
            params['NAME'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + targetName };
        }

        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });

        obj.res = result.rows;
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'getStock', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function updateStock(user, targetName, safetyStock, stockMax) {
    const obj = {
        res: [],
        error: '',
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            UPDATE PBTC_IOT_STOCK_SETTING
                SET SAFETY_STOCK = :SAFETY_STOCK,
                    STOCK_MAX = :STOCK_MAX,
                    EDITOR = :EDITOR,
                    EDIT_DATE = SYSDATE
                WHERE COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT
                AND NAME = :NAME `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.NAME },
            NAME: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + targetName },
            SAFETY_STOCK: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(safetyStock) },
            STOCK_MAX: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(stockMax) },
        };

        const result = await conn.execute(sql, params, { autoCommit: false });

        if (result.rowsAffected) {
            await conn.commit();
        } else {
            throw new Error('庫存更新失敗');
        }
    } catch (err) {
        console.error(libs.getNowDatetimeString(), 'updateStock', err);
        obj.error = err.toString();
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}
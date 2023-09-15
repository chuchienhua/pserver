import config from '../config.js';
import { getNowDatetimeString } from '../libs.js';
import oracledb from 'oracledb';
import * as Mailer from '../mailer.js';

/* 配方管理部份 */
//取得配方詳細表
export async function getRecipesDetail(productNo, version, line, category, series, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        let sql = `
            SELECT *
            FROM PBTC_IOT_RECIPE_DETAIL
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('*' !== productNo) {
            sql += ' AND PRODUCT_NO = :PRODUCT_NO ';
            params['PRODUCT_NO'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() };
        }
        if ('*' !== version) {
            sql += ' AND VER = :VER ';
            params['VER'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() };
        }
        if ('*' !== line) {
            sql += ' AND LINE = :LINE ';
            params['LINE'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() };
        }
        if ('*' !== category) {
            sql += ' AND CATEGORY = :CATEGORY ';
            params['CATEGORY'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: category.toString() };
        }
        if ('*' !== series) {
            sql += ' AND SERIES = :SERIES ';
            params['SERIES'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: series.toString() };
        }

        sql += 'ORDER BY PRODUCT_NO, VER, LINE ';
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRecipesDetail', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得配方比例
export async function getRecipe(productNo, version, line, user) {
    let obj = {
        detail: [],
        ratio: [],
        error: false,
    };

    let sql;
    let params;
    let conn;
    let result;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //取得寫入ERP資料與規格檔
        sql = `
            SELECT PRODUCT_NO, VER, LINE, PRO_FORMULA_BATCH_WT, PRO_FORMULA_UPBOND, PRO_LINE_OUTPUT_QTY, CATEGORY, SERIES, COLOR
            FROM PBTC_IOT_RECIPE_DETAIL
            WHERE PRODUCT_NO = :PRODUCT_NO 
            AND VER = :VER
            AND LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.detail = result.rows;

        //取得配方比例表
        sql = `
            SELECT MATERIAL, RATIO, FEEDER, MIXER, SEMI_NO, SUPPLIER, RESIN_TYPE, FR_TYPE, GF_TYPE, AIRINPUT, MIDDLE
            FROM PBTC_IOT_RECIPE
            WHERE PRODUCT_NO = :PRODUCT_NO 
            AND VER = :VER
            AND LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND CREATE_TIME = ( SELECT MAX( CREATE_TIME ) 
                                FROM PBTC_IOT_RECIPE
                                WHERE PRODUCT_NO = :PRODUCT_NO
                                AND VER = :VER
                                AND LINE = :LINE
                                AND COMPANY = :COMPANY
                                AND FIRM = :FIRM  )
            ORDER BY FEEDER `;
        result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.ratio = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRecipe', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//新增配方檔
export async function createRecipe(productNo, version, line, erpData, specData, materialArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date();
    let result;

    //erpData資料
    const batchWeight = erpData.batchWeight;
    const productivity = erpData.productivity;
    const extWeight = erpData.extWeight;
    const resinRate = erpData.resinRate;
    const resinType = erpData.resinType;
    const frRate = erpData.frRate;
    const frType = erpData.frType;
    const gfRate = erpData.gfRate;
    const gfType = erpData.gfType;

    //specData規格資料
    const category = specData.category;
    const series = specData.series;
    const color = specData.color;

    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const isAdmin = await checkIsAdmin(user, 'createRecipe');
        if (!isAdmin) {
            throw new Error(`${user.PPS_CODE}並無此廠${user.FIRM}的配方管理員權限`);
        }

        sql = `
            INSERT INTO PBTC_IOT_RECIPE_DETAIL ( 
                PRODUCT_NO, VER, LINE, PRO_FORMULA_BATCH_WT, PRO_FORMULA_UPBOND, PRO_LINE_OUTPUT_QTY, 
                RESIN_RATE, RESIN_TYPE, FR_RATE, FR_TYPE, GF_RATE, GF_TYPE,
                CATEGORY, SERIES, COLOR,
                CREATOR, CREATE_TIME, COMPANY, FIRM )
            VALUES ( 
                :PRODUCT_NO, :VER, :LINE, :PRO_FORMULA_BATCH_WT, :PRO_FORMULA_UPBOND, :PRO_LINE_OUTPUT_QTY, 
                :RESIN_RATE, :RESIN_TYPE, :FR_RATE, :FR_TYPE, :GF_RATE, :GF_TYPE,
                :CATEGORY, :SERIES, :COLOR,
                :CREATOR, :CREATE_TIME, :COMPANY, :FIRM ) `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            PRO_FORMULA_BATCH_WT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchWeight) },
            PRO_FORMULA_UPBOND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(productivity) },
            PRO_LINE_OUTPUT_QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(extWeight) },
            RESIN_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(resinRate) },
            FR_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(frRate) },
            GF_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(gfRate) },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE.toString() },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
            RESIN_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: resinType ? resinType : '' },
            FR_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: frType ? frType : '' },
            GF_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: gfType ? gfType : '' },
            CATEGORY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: category ? category : '' },
            SERIES: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: series ? series : '' },
            COLOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: color ? color : '' },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const commit = { autoCommit: false };
        result = await conn.execute(sql, params, commit);

        if (result.rowsAffected) {
            //新增比例至 PBTC_IOT_RECIPE
            for (const material of materialArray) {
                sql = `
                    INSERT INTO PBTC_IOT_RECIPE ( PRODUCT_NO, VER, LINE, MATERIAL, RATIO, FEEDER, MIXER, SEMI_NO, CREATE_TIME, COMPANY, FIRM, RESIN_TYPE, FR_TYPE, GF_TYPE, AIRINPUT, MIDDLE )
                    VALUES ( :PRODUCT_NO, :VER, :LINE, :MATERIAL, :RATIO, :FEEDER, :MIXER, :SEMI_NO, :CREATE_TIME, :COMPANY, :FIRM, :RESIN_TYPE, :FR_TYPE, :GF_TYPE, :AIRINPUT, :MIDDLE ) `;
                params = {
                    PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
                    VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                    MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                    RATIO: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(material.RATIO) },
                    FEEDER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.FEEDER.toString() },
                    MIXER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.MIXER) ? material.MIXER.toString() : null },
                    SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.SEMI_NO) ? material.SEMI_NO.toString() : 'M' },
                    CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    RESIN_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.RESIN) ? 'Y' : 'N' },
                    FR_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.FR) ? 'Y' : 'N' },
                    GF_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.GF) ? 'Y' : 'N' },
                    AIRINPUT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.AIRINPUT) ? 'Y' : 'N' },
                    MIDDLE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.MIDDLE) ? 'Y' : 'N' },
                };
                const commit = { autoCommit: false };
                await conn.execute(sql, params, commit);
            }

            const syncError = await syncProTable(productNo, version, line, batchWeight, productivity, extWeight, resinRate, resinType, frRate, frType, gfRate, gfType, user);
            if (syncError) {
                throw new Error('同步至PRO_TABLE失敗');
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'createRecipe', err);
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

//更新配方檔
export async function updateRecipe(updateProductNo, updateVersion, updateLine, erpData, specData, materialArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date();
    let result;

    //erpData資料
    const batchWeight = erpData.batchWeight;
    const productivity = erpData.productivity;
    const extWeight = erpData.extWeight;
    const resinRate = erpData.resinRate;
    const resinType = erpData.resinType;
    const frRate = erpData.frRate;
    const frType = erpData.frType;
    const gfRate = erpData.gfRate;
    const gfType = erpData.gfType;

    //specData規格資料
    const category = specData.category;
    const series = specData.series;
    const color = specData.color;

    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const isAdmin = await checkIsAdmin(user, 'updateRecipe');
        if (!isAdmin) {
            throw new Error(`${user.PPS_CODE}並無此廠${user.FIRM}的配方管理員權限`);
        }

        //移除舊的配方比例表
        sql = `
            DELETE PBTC_IOT_RECIPE
            WHERE PRODUCT_NO = :PRODUCT_NO
            AND VER = :VER
            AND LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateProductNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateVersion.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateLine.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: false });

        sql = `
            UPDATE PBTC_IOT_RECIPE_DETAIL
            SET PRO_FORMULA_BATCH_WT = :PRO_FORMULA_BATCH_WT, 
                PRO_FORMULA_UPBOND = :PRO_FORMULA_UPBOND, 
                PRO_LINE_OUTPUT_QTY = :PRO_LINE_OUTPUT_QTY,
                RESIN_RATE = :RESIN_RATE,
                RESIN_TYPE = :RESIN_TYPE,
                FR_RATE = :FR_RATE,
                FR_TYPE = :FR_TYPE,
                GF_RATE = :GF_RATE,
                GF_TYPE = :GF_TYPE,
                CATEGORY = :CATEGORY,
                SERIES = :SERIES,
                COLOR = :COLOR,
                EDITOR = :EDITOR,
                ALTER_TIME = :ALTER_TIME
            WHERE PRODUCT_NO = :PRODUCT_NO
            AND VER = :VER
            AND LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateProductNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateVersion.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateLine.toString() },
            PRO_FORMULA_BATCH_WT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchWeight) },
            PRO_FORMULA_UPBOND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(productivity) },
            PRO_LINE_OUTPUT_QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(extWeight) },
            RESIN_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(resinRate) },
            FR_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(frRate) },
            GF_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(gfRate) },
            RESIN_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: resinType ? resinType : '' },
            FR_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: frType ? frType : '' },
            GF_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: gfType ? gfType : '' },
            CATEGORY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: category ? category : '' },
            SERIES: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: series ? series : '' },
            COLOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: color ? color : '' },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            ALTER_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        result = await conn.execute(sql, params, { autoCommit: false });

        if (result.rowsAffected) {
            for (const material of materialArray) {
                sql = `
                    INSERT INTO PBTC_IOT_RECIPE 
                        ( PRODUCT_NO, VER, LINE, MATERIAL, RATIO, FEEDER, MIXER, SEMI_NO, CREATE_TIME, COMPANY, FIRM, RESIN_TYPE, FR_TYPE, GF_TYPE, AIRINPUT, MIDDLE )
                    VALUES 
                        ( :PRODUCT_NO, :VER, :LINE, :MATERIAL, :RATIO, :FEEDER, :MIXER, :SEMI_NO, :CREATE_TIME, :COMPANY, :FIRM, :RESIN_TYPE, :FR_TYPE, :GF_TYPE, :AIRINPUT, :MIDDLE ) `;
                params = {
                    PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateProductNo.toString() },
                    VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateVersion.toString() },
                    LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateLine.toString() },
                    MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.MATERIAL.toString() },
                    RATIO: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(material.RATIO) },
                    FEEDER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.FEEDER.toString() },
                    MIXER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.MIXER) ? material.MIXER.toString() : null },
                    SEMI_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.SEMI_NO) ? material.SEMI_NO.toString() : 'M' },
                    COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                    FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                    CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                    RESIN_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.RESIN) ? 'Y' : 'N' },
                    FR_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.FR) ? 'Y' : 'N' },
                    GF_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.GF) ? 'Y' : 'N' },
                    AIRINPUT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.AIRINPUT) ? 'Y' : 'N' },
                    MIDDLE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: (material.MIDDLE) ? 'Y' : 'N' },
                };
                await conn.execute(sql, params, { autoCommit: false });
            }

            const syncError = await syncProTable(updateProductNo, updateVersion, updateLine, batchWeight, productivity, extWeight, resinRate, resinType, frRate, frType, gfRate, gfType, user);
            if (syncError) {
                throw new Error('同步至PRO_TABLE失敗');
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateRecipe', err);
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

//複製配方
export async function copyRecipe(productNo, copyVersion, copyLine, erpData, newProductNo, newVersion, newLine, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    let date = new Date();
    let result;

    //erpData資料
    const batchWeight = erpData.batchWeight;
    const productivity = erpData.productivity;
    const extWeight = erpData.extWeight;
    const resinRate = erpData.resinRate;
    const resinType = erpData.resinType;
    const frRate = erpData.frRate;
    const frType = erpData.frType;
    const gfRate = erpData.gfRate;
    const gfType = erpData.gfType;

    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const isAdmin = await checkIsAdmin(user, 'copyRecipe');
        if (!isAdmin) {
            throw new Error(`${user.PPS_CODE}並無此廠${user.FIRM}的配方管理員權限`);
        }

        sql = `
            INSERT INTO PBTC_IOT_RECIPE_DETAIL ( 
                PRODUCT_NO, VER, LINE, PRO_FORMULA_BATCH_WT, PRO_FORMULA_UPBOND, PRO_LINE_OUTPUT_QTY, 
                RESIN_RATE, FR_RATE, GF_RATE, CREATOR, CREATE_TIME, COMPANY, FIRM, RESIN_TYPE, FR_TYPE, GF_TYPE )
            VALUES ( 
                :PRODUCT_NO, :VER, :LINE, :PRO_FORMULA_BATCH_WT, :PRO_FORMULA_UPBOND, :PRO_LINE_OUTPUT_QTY, 
                :RESIN_RATE, :FR_RATE, :GF_RATE, :CREATOR, :CREATE_TIME, :COMPANY, :FIRM, :RESIN_TYPE, :FR_TYPE, :GF_TYPE ) `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newProductNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newVersion.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newLine.toString() },
            PRO_FORMULA_BATCH_WT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchWeight) },
            PRO_FORMULA_UPBOND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(productivity) },
            PRO_LINE_OUTPUT_QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(extWeight) },
            RESIN_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(resinRate) },
            FR_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(frRate) },
            GF_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(gfRate) },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: user.PPS_CODE.toString() },
            CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            RESIN_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: resinType ? resinType : '' },
            FR_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: frType ? frType : '' },
            GF_TYPE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: gfType ? gfType : '' },
        };
        result = await conn.execute(sql, params, { autoCommit: false });

        if (result.rowsAffected) {
            sql = `
                INSERT INTO PBTC_IOT_RECIPE ( 
                    PRODUCT_NO, VER, LINE, MATERIAL, RATIO, FEEDER, MIXER, SEMI_NO, SUPPLIER, CREATE_TIME, COMPANY, FIRM, RESIN_TYPE, FR_TYPE, GF_TYPE, AIRINPUT, MIDDLE )
                SELECT 
                    :NEW_PRODUCT_NO, :NEW_VER, :NEW_LINE, MATERIAL, RATIO, 
                    REPLACE(FEEDER, :COPY_LINE, :NEW_LINE ), 
                    MIXER, SEMI_NO, SUPPLIER, :CREATE_TIME, :COMPANY, :FIRM,
                    RESIN_TYPE, FR_TYPE, GF_TYPE, AIRINPUT, MIDDLE
                FROM PBTC_IOT_RECIPE
                WHERE PRODUCT_NO = :COPY_PRODUCT_NO 
                AND VER = :COPY_VER
                AND LINE = :COPY_LINE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND CREATE_TIME = ( SELECT MAX( CREATE_TIME ) 
                                    FROM PBTC_IOT_RECIPE
                                    WHERE PRODUCT_NO = :COPY_PRODUCT_NO 
                                    AND VER = :COPY_VER
                                    AND LINE = :COPY_LINE
                                    AND COMPANY = :COMPANY
                                    AND FIRM = :FIRM ) `;
            params = {
                COPY_PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
                NEW_PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newProductNo.toString() },
                NEW_VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newVersion.toString() },
                NEW_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: newLine.toString() },
                COPY_VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: copyVersion.toString() },
                COPY_LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: copyLine.toString() },
                CREATE_TIME: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: date },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });

            const syncError = await syncProTable(newProductNo, newVersion, newLine, batchWeight, productivity, extWeight, resinRate, resinType, frRate, frType, gfRate, gfType, user);
            if (syncError) {
                throw new Error('同步至PRO_TABLE失敗');
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'copyRecipe', err);
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

//刪除配方
export async function deleteRecipe(productNo, version, line, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        const isAdmin = await checkIsAdmin(user, 'deleteRecipe');
        if (!isAdmin) {
            throw new Error(`${user.PPS_CODE}並無此廠${user.FIRM}的配方管理員權限`);
        }

        sql = `
            DELETE PBTC_IOT_RECIPE_DETAIL
            WHERE PRODUCT_NO = :PRODUCT_NO
            AND VER = :VER
            AND LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        let res = await conn.execute(sql, params, { autoCommit: false });

        if (res.rowsAffected) {
            sql = ` 
                DELETE PBTC_IOT_RECIPE
                WHERE PRODUCT_NO = :PRODUCT_NO
                AND VER = :VER
                AND LINE = :LINE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM `;
            params = {
                PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
                VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });

            //移除江課TABLE資料
            sql = `
                DELETE PRO_LINE_OUTPUT
                WHERE PRD_PC = :PRD_PC
                AND LINE = :LINE
                AND COMPANY = :COMPANY
                AND FIRM = :FIRM
                AND DEPT = :DEPT `;
            params = {
                PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
                DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'deleteRecipe', err);
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

//寫入批次量/產能上限/押出量至江課Table
async function syncProTable(productNo, version, line, batchWeight, productivity, extWeight, resinRate, resinType, frRate, frType, gfRate, gfType, user) {
    let error = false;
    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        sql = `
            MERGE INTO PRO_FORMULA A
            USING ( SELECT :PRD_PC PRD_PC, :COMPANY COMPANY, :FIRM FIRM, :DEPT DEPT FROM DUAL ) B
            ON ( A.PRD_PC = B.PRD_PC AND A.COMPANY = B.COMPANY AND A.FIRM = B.FIRM AND A.DEPT = B.DEPT )
            WHEN MATCHED THEN
                UPDATE SET BATCH_WT = :BATCH_WT, UPBOND = :UPBOND, RESIN_RATE = :RESIN_RATE, FR_RATE = :FR_RATE, GF_RATE = :GF_RATE, RESIN = :RESIN, FR_KIND = :FR_KIND, GLASSFIBER = :GLASSFIBER
            WHEN NOT MATCHED THEN
                INSERT ( PRD_PC, SPEC, BATCH_WT, UPBOND, RESIN_RATE, FR_RATE, GF_RATE, CREATOR, CRT_DATE, COMPANY, FIRM, DEPT, RESIN, FR_KIND, GLASSFIBER )
                VALUES ( :PRD_PC, :SPEC, :BATCH_WT, :UPBOND, :RESIN_RATE, :FR_RATE, :GF_RATE, :CREATOR, :CRT_DATE, :COMPANY, :FIRM, :DEPT, :RESIN, :FR_KIND, :GLASSFIBER ) `;
        params = {
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            SPEC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            BATCH_WT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(batchWeight) },
            UPBOND: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(productivity) },
            RESIN_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(resinRate) },
            FR_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(frRate) },
            GF_RATE: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(gfRate) },
            CRT_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
            RESIN: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: resinType || '' },
            FR_KIND: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: frType || '' },
            GLASSFIBER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: gfType || '' },
        };
        await conn.execute(sql, params, { autoCommit: true });

        sql = `
            BEGIN
                INSERT INTO PRO_LINE_OUTPUT (
                    PRD_PC, LINE, OUTPUT_QTY,
                    CREATOR, CRT_DATE,
                    COMPANY, FIRM, DEPT )
                VALUES (
                    :PRD_PC, :LINE, :OUTPUT_QTY,
                    :CREATOR, :CRT_DATE,
                    :COMPANY, :FIRM, :DEPT );
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE PRO_LINE_OUTPUT
                    SET OUTPUT_QTY = :OUTPUT_QTY
                    WHERE PRD_PC = :PRD_PC
                    AND LINE = :LINE
                    AND COMPANY = :COMPANY
                    AND FIRM = :FIRM
                    AND DEPT = :DEPT;
            END; `;
        params = {
            PRD_PC: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            OUTPUT_QTY: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(extWeight) },
            CRT_DATE: { dir: oracledb.BIND_IN, type: oracledb.DATE, val: new Date() },
            CREATOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            DEPT: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.DEPT },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'syncIntoProTable', err);
        error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return error;
}



/* 原料管理部分 */
//取得所有原料
export async function getMaterials(user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        //現場人員原料要改用自己選擇的，不抓DB的
        const sql = `
            SELECT CODE, QC
            FROM PBTC_IOT_MATERIAL
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY CODE `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        /*
        const sql = `
        SELECT
        CCFIL080BOM.MATER_AB AS CODE
        FROM CCFIL080BOM
        WHERE CCFIL080BOM.COMPANY = '1'
        AND CCFIL080BOM.FIRM = '7'
        AND CCFIL080BOM.YYYYMM = :YYYYMM
        GROUP BY CCFIL080BOM.MATER_AB `;
        const params = {
            YYYYMM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: moment(new Date()).format('YYYYMM') },
        }
        */

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
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

//取得原料餘料量
export async function getRemainder(material, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        let sql = `
            SELECT MATERIAL, WEIGHT, BATCH_NO, LOT_NO
            FROM PBTC_IOT_REMAINDER
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        let params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        if ('*' !== material) {
            sql += ' AND MATERIAL = :MATERIAL ';
            params['MATERIAL'] = { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() };
        }

        sql += 'ORDER BY MATERIAL ';
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRemainder', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新原料餘料量
export async function updateRemainder(material, weight, batchNo, lotNo, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            UPDATE PBTC_IOT_REMAINDER
            SET WEIGHT = :WEIGHT,
                BATCH_NO = :BATCH_NO,
                LOT_NO = :LOT_NO,
                EDITOR = :EDITOR
            WHERE MATERIAL = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(weight) },
            BATCH_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: batchNo || '' },
            LOT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: lotNo || '' },
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + material },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            EDITOR: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateRemainder', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//新增原料
export async function createMaterial(material, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_MATERIAL ( CODE, COMPANY, FIRM )
            VALUES ( :MATERIAL, :COMPANY, :FIRM ) `;
        const params = {
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'createMaterial', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//新增原料餘料
export async function createRemainder(material, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            INSERT INTO PBTC_IOT_REMAINDER ( MATERIAL, COMPANY, FIRM )
            VALUES ( :MATERIAL, :COMPANY, :FIRM ) `;
        const params = {
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'createRemainder', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//刪除原料
export async function removeMaterial(material, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            DELETE PBTC_IOT_MATERIAL
            WHERE CODE = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeMaterial', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//刪除原料餘料
export async function removeRemainder(material, user) {
    let obj = {
        res: [],
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            DELETE PBTC_IOT_REMAINDER
            WHERE MATERIAL = :MATERIAL
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            MATERIAL: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: material.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        await conn.execute(sql, params, { autoCommit: true });
    } catch (err) {
        console.error(getNowDatetimeString(), 'removeRemainder', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得指定押出製造標準
export async function getRecipeStandard(productNo, version, line, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT B.SEQUENCE, B.TYPE, A.TOLERANCE, A.BASE
            FROM PBTC_IOT_EXTRUSION_STD A, PBTC_IOT_EXTRUSION_STD_TYPE B
            WHERE A.SEQUENCE = B.SEQUENCE
            AND A.PRODUCT_NO = :PRODUCT_NO
            AND A.VER = :VER
            AND A.LINE = :LINE
            AND A.COMPANY = :COMPANY
            AND A.FIRM = :FIRM
            ORDER BY A.SEQUENCE `;
        const params = {
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: productNo.toString() },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: version.toString() },
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRecipeStandard', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新指定押出製造標準
export async function updateRecipeStandard(updateProductNo, updateVersion, updateLine, stdArray, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    let sql;
    let params;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);

        for (const standard of stdArray) {
            sql = `
                BEGIN
                    INSERT INTO PBTC_IOT_EXTRUSION_STD (
                        PRODUCT_NO, VER, LINE, 
                        SEQUENCE, TOLERANCE, BASE,
                        COMPANY, FIRM )
                    VALUES (
                        :PRODUCT_NO, :VER, :LINE, 
                        :SEQUENCE, :TOLERANCE, :BASE,
                        :COMPANY, :FIRM );
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE PBTC_IOT_EXTRUSION_STD
                        SET TOLERANCE = :TOLERANCE,
                            BASE = :BASE
                        WHERE PRODUCT_NO = :PRODUCT_NO
                        AND VER = :VER
                        AND LINE = :LINE
                        AND SEQUENCE = :SEQUENCE
                        AND COMPANY = :COMPANY
                        AND FIRM = :FIRM;
                END; `;
            params = {
                PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateProductNo.toString() },
                VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateVersion.toString() },
                LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: updateLine.toString() },
                SEQUENCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: standard.SEQUENCE.toString() },
                TOLERANCE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: standard.TOLERANCE.toString() },
                BASE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: standard.BASE.toString() },
                COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
                FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            };
            await conn.execute(sql, params, { autoCommit: false });
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateRecipeStandard', err);
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

export async function getFileMaintain(user) {
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
            SELECT PRODUCT_NO, LINE, VER, BAG_WEIGHT
            FROM PBTC_IOT_RECIPE_DETAIL
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            ORDER BY PRODUCT_NO `;
        params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };

        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getFileMaintain error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//取得稽核用的產品簡碼
export async function getAuditProduct(user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT PRODUCT_NO
            FROM PBTC_IOT_RECIPE_DETAIL
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND AUDIT_PRODUCT = 'Y'
            GROUP BY PRODUCT_NO
            ORDER BY PRODUCT_NO `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        obj.res = result.rows;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getAuditProduct error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//更新稽核用的產品簡碼
export async function updateAuditProduct(productNo, type, user) {
    let obj = {
        res: null,
        error: false,
    };

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            UPDATE PBTC_IOT_RECIPE_DETAIL
            SET AUDIT_PRODUCT = '${'remove' === type ? 'N' : 'Y'}'
            WHERE COMPANY = :COMPANY
            AND FIRM = :FIRM
            AND PRODUCT_NO = :PRODUCT_NO `;
        const params = {
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo },
        };
        const result = await conn.execute(sql, params, { autoCommit: true });
        if (!result.rowsAffected) {
            throw new Error('沒有符合的產品簡碼');
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'updateAuditProduct error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

export async function editBagWeight(productNo, line, ver, bagWeight, user) {
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
            UPDATE PBTC_IOT_RECIPE_DETAIL
            SET BAG_WEIGHT = :BAG_WEIGHT
            WHERE LINE = :LINE
            AND VER = :VER
            AND PRODUCT_NO = :PRODUCT_NO
            AND FIRM = :FIRM
            AND COMPANY = :COMPANY `;
        params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + line },
            VER: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + ver },
            PRODUCT_NO: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + productNo },
            BAG_WEIGHT: { dir: oracledb.BIND_IN, type: oracledb.NUMBER, val: Number(bagWeight) },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY }
        };
        const commit = { autoCommit: true };
        await conn.execute(sql, params, commit);
    } catch (err) {
        console.error(getNowDatetimeString(), 'editBagWeight error', err);
        obj.res = err.toString();
        obj.error = true;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return obj;
}

//目前有跨廠需求，每次異動都先檢查是否有該廠權限
async function checkIsAdmin(user, functionName) {
    let isAdmin = false;
    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT PPS_CODE
            FROM PBTC_IOT_AUTH
            WHERE ROUTE = 'recipe'
            AND ISADMIN = '1'
            AND PPS_CODE = :PPS_CODE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            PPS_CODE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.PPS_CODE },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const result = await conn.execute(sql, params, { outFormat: oracledb.OBJECT });
        if (result.rows.length) {
            isAdmin = true;
        } else {
            console.log('此人並無權限，將寄信');
            await Mailer.recipeAuthAlarm(user, functionName);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'checkIsAdmin error', err);
        isAdmin = false;
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return isAdmin;
}
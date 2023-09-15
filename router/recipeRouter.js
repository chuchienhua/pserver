import * as recipeDB from '../recipe/oracleRecipe.js';
import express from 'express';

const recipeRouter = express.Router();

/* 配方管理功能 */
//取得指定配方詳細
recipeRouter.post('/detail/:productNo', function (req, res) {
    const productNo = req.params.productNo;
    const version = req.body.version;
    const line = req.body.line;
    const category = req.body.category;
    const series = req.body.series;

    recipeDB.getRecipesDetail(productNo, version, line, category, series, req.user)
        .then(val => res.send(val));
});

//取得指定配方比例
recipeRouter.get('/ratio/:productNo/:version/:line', function (req, res) {
    const productNo = req.params.productNo;
    const version = req.params.version;
    const line = req.params.line;

    recipeDB.getRecipe(productNo, version, line, req.user)
        .then(val => res.send(val));
});

//新增or更新配方
recipeRouter.post('/create/:updateProductNo?/:updateVersion?/:updateLine?', function (req, res) {
    const updateProductNo = req.params.updateProductNo;
    const updateVersion = req.params.updateVersion;
    const updateLine = req.params.updateLine;

    const productNo = req.body.productNo;
    const version = req.body.version;
    const line = req.body.line;
    const erpData = req.body.erpData; //含有batchWeight、productivity、extWeight、resinRate、resinType、frRate、frType、gfRate、gfType
    const specData = req.body.specData; //含有category, series, color
    const materialArray = req.body.materialArray;

    if (updateProductNo && updateVersion && updateLine) {
        recipeDB.updateRecipe(updateProductNo, updateVersion, updateLine, erpData, specData, materialArray, req.user)
            .then(val => res.send(val));
    } else {
        recipeDB.createRecipe(productNo, version, line, erpData, specData, materialArray, req.user)
            .then(val => res.send(val));
    }
});

//複製配方至其他線別
recipeRouter.post('/copy', function (req, res) {
    const productNo = req.body.copyProductNo;
    const copyVersion = req.body.copyVersion;
    const copyLine = req.body.copyLine;
    const erpData = req.body.erpData;
    const newProductNo = req.body.newProductNo;
    const newVersion = req.body.newVersion;
    const newLine = req.body.newLine;

    recipeDB.copyRecipe(productNo, copyVersion, copyLine, erpData, newProductNo, newVersion, newLine, req.user)
        .then(val => res.send(val));
});

//刪除配方
recipeRouter.get('/delete/:productNo/:version/:line', function (req, res) {
    const productNo = req.params.productNo;
    const version = req.params.version;
    const line = req.params.line;

    recipeDB.deleteRecipe(productNo, version, line, req.user)
        .then(val => res.send(val));
});

//取得所有配方能用的原料
recipeRouter.get('/getMaterials', function (req, res) {
    recipeDB.getMaterials(req.user)
        .then(val => res.send(val));
});

//取得指定押出製造標準
recipeRouter.get('/standard/:productNo/:version/:line', function (req, res) {
    const productNo = req.params.productNo;
    const version = req.params.version;
    const line = req.params.line;

    recipeDB.getRecipeStandard(productNo, version, line, req.user)
        .then(val => res.send(val));
});

//更新指定押出製造標準
recipeRouter.post('/updateStandard/:productNo/:version/:line', function (req, res) {
    const productNo = req.params.productNo;
    const version = req.params.version;
    const line = req.params.line;

    const stdArray = req.body.stdArray;

    recipeDB.updateRecipeStandard(productNo, version, line, stdArray, req.user)
        .then(val => res.send(val));
});

/* 原料/餘料管理 */
//取得原料餘料量
recipeRouter.get('/remainder/:material', function (req, res) {
    const material = req.params.material;

    recipeDB.getRemainder(material, req.user)
        .then(val => res.send(val));
});

//更新原料餘料量
recipeRouter.post('/updateRemainder', function (req, res) {
    const material = req.body.material;
    const weight = req.body.weight;
    const batchNo = req.body.batchNo;
    const lotNo = req.body.lotNo;

    recipeDB.updateRemainder(material, weight, batchNo, lotNo, req.user)
        .then(val => res.send(val));
});

//原料/餘料新增刪除動作
recipeRouter.get('/materialManage/:tableType/:operation/:material', function (req, res) {
    const tableType = req.params.tableType;
    const operation = req.params.operation;
    const material = req.params.material;

    if ('material' === tableType) {
        if ('remove' === operation) {
            recipeDB.removeMaterial(material, req.user)
                .then(val => res.send(val));

        } else if ('create' === operation) {
            recipeDB.createMaterial(material, req.user)
                .then(val => res.send(val));
        }

    } else if ('remainder' === tableType) {
        if ('remove' === operation) {
            recipeDB.removeRemainder(material, req.user)
                .then(val => res.send(val));

        } else if ('create' === operation) {
            recipeDB.createRemainder(material, req.user)
                .then(val => res.send(val));
        }

    }
});

//檔案維護取得半成品袋重
recipeRouter.get('/fileMaintain', function (req, res) {
    recipeDB.getFileMaintain(req.user)
        .then(val => res.send(val));
});

//取得稽核用的產品簡碼
recipeRouter.get('/getAuditProduct', function (req, res) {
    recipeDB.getAuditProduct(req.user)
        .then(val => res.send(val));
});

//更新稽核用的產品簡碼
recipeRouter.post('/updateAuditProduct/:type', function (req, res) {
    const type = req.params.type;
    const productNo = req.body.productNo;

    recipeDB.updateAuditProduct(productNo, type, req.user)
        .then(val => res.send(val));
});

export default recipeRouter;
import * as stockDB from '../stockSetting/oracleStockSetting.js';
import express from 'express';

const stockSettingRouter = express.Router();

/* 原料/產品安全庫存與上限維護 */
//取得所有庫存資訊
stockSettingRouter.post('/getStock', function (req, res) {
    const targetName = req.body.targetName;
    const searchType = req.body.searchType;
    stockDB.getStock(req.user, targetName, searchType)
        .then(val => {
            res.send(val);
        });
});

//更新指定庫存檔案
stockSettingRouter.post('/updateStock/:targetName', function (req, res) {
    const targetName = req.params.targetName;
    const safetyStock = req.body.safetyStock;
    const stockMax = req.body.stockMax;

    stockDB.updateStock(req.user, targetName, safetyStock, stockMax)
        .then(val => {
            res.send(val);
        });
});

export default stockSettingRouter;
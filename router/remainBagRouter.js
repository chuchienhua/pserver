import * as remainBagDB from '../packing/oraclePackingRemain.js';
import express from 'express';

const remainBagRouter = express.Router();

//查詢殘包儲存格位資訊
remainBagRouter.post('/getLONOData', function (req, res) {
    const lono = req.body.LONO;

    remainBagDB.getLONOData(req.user, lono)
        .then(val => {
            res.send(val);
        });
});

remainBagRouter.post('/getBagData', function (req, res) {
    const opno = req.body.OPNO;

    remainBagDB.getBagData(req.user, opno)
        .then(val => {
            res.send(val);
        });
});

//殘包入庫
remainBagRouter.post('/bagInStorage', function (req, res) {
    const lono = req.body.LONO;
    const opno = req.body.OPNO;

    remainBagDB.bagInStorage(req.user, opno, lono)
        .then(val => {
            res.send(val);
        });
});

//殘包出庫
remainBagRouter.post('/bagOutStorage', function (req, res) {
    const lono = req.body.LONO;
    const opno = req.body.OPNO;
    const reason = req.body.reason;

    remainBagDB.bagOutStorage(req.user, opno, lono, reason)
        .then(val => {
            res.send(val);
        });
});

//列印格位標籤
remainBagRouter.post('/printLonoLabel', function (req, res) {
    const printData = req.body.lonoData;
    const printerIP = req.body.printerIP;

    remainBagDB.printLonoLabel(req.user, printerIP, printData)
        .then(val => {
            res.send(val);
        });
});

//列印殘包標籤 - 包裝性質待調整
remainBagRouter.post('/printBagLabel', function (req, res) {
    const productNo = req.body.bagPRD_PC;
    const lotno = req.body.bagLOT_NO;
    const property = req.body.bagProperty;
    const weight = req.body.bagWeight;
    const printerIP = req.body.printerIP;

    remainBagDB.printBagLabel(req.user, productNo, lotno, property, weight, printerIP)
        .then(val => {
            res.send(val);
        });
});

//儲存殘包資訊
remainBagRouter.post('/storeBagInfo', function (req, res) {
    const productNo = req.body.bagPRD_PC;
    const lotno = req.body.bagLOT_NO;
    const property = req.body.bagProperty;
    const weight = req.body.bagWeight;
    const lono = req.body.lonoData;

    remainBagDB.storeBagInfo(req.user, productNo, lotno, property, weight, lono)
        .then(val => {
            res.send(val);
        });
});

//檢查產品簡碼與批號
remainBagRouter.post('/confirmProSchedule', function (req, res) {
    const productNo = req.body.bagPRD_PC;
    const lotno = req.body.bagLOT_NO;

    remainBagDB.confirmProSchedule(req.user, productNo, lotno)
        .then(val => {
            res.send(val);
        });
});

//更新LOG
remainBagRouter.post('/updateBagInfo', function (req, res) {
    const opno = req.body.OPNO;
    const productNo = req.body.prd_pc;
    const lotno = req.body.lot_no;
    const weight = req.body.weight;
    const lono = req.body.LONO;
    const reason = req.body.reason;
    const doingType = req.body.doingType;

    remainBagDB.updateBagInfo(req.user, doingType, productNo, lotno, weight, lono, opno, reason)
        .then(val => {
            res.send(val);
        });
});

//查詢庫存
remainBagRouter.post('/queryStock', function (req, res) {
    const productNo = req.body.PRD_PC;
    const lotno = req.body.LOT_NO;
    const lono = req.body.LONO;

    remainBagDB.queryStock(req.user, productNo, lotno, lono)
        .then(val => {
            res.send(val);
        });
});

//殘包管理報表(週報)
remainBagRouter.post('/queryBagReport', function (req, res) {
    const startDate = req.body.newStartDate;
    const endDate = req.body.newEndDate;

    remainBagDB.queryBagWeekReport(req.user, startDate, endDate)
        .then(val => {
            res.send(val);
        });
});

//殘包管理報表(月報)
remainBagRouter.post('/queryBagMonthReport', function (req, res) {
    const startDate = req.body.newStartDate;
    const endDate = req.body.newEndDate;

    remainBagDB.queryBagMonthReport(req.user, startDate, endDate)
        .then(val => {
            res.send(val);
        });
});

//殘包管理報表(匯出)
remainBagRouter.post('/exportBagReport', function (req, res) {
    const data = req.body.reportData;
    const thisMonth = req.body.thisMonth;
    const lastMonth = req.body.lastMonth;
    const type = req.body.reportType;
    
    remainBagDB.exportExcel(data, type, thisMonth, lastMonth)
        .then(val => {
            res.send(val);
        });
});

//查詢異常庫存
remainBagRouter.post('/queryErrorStock', function (req, res) {
    const productNo = req.body.PRD_PC;
    const lotno = req.body.LOT_NO;
    const startDate = req.body.newStartDate;
    const endDate = req.body.newEndDate;

    remainBagDB.queryErrorStock(req.user, productNo, lotno, startDate, endDate)
        .then(val => {
            res.send(val);
        });
});

//查詢殘包進出記錄
remainBagRouter.post('/queryStatus', function (req, res) {
    const startDate = req.body.newStartDate;
    const endDate = req.body.newEndDate;
    const productNo = req.body.PRD_PC;
    const lotNo = req.body.LOT_NO;

    remainBagDB.queryStatus(req.user, startDate, endDate, productNo, lotNo)
        .then(val => {
            res.send(val);
        });
});

//格位盤點
remainBagRouter.get('/queryLonoStatus', function (req, res) {

    remainBagDB.queryLonoStatus(req.user)
        .then(val => {
            res.send(val);
        });
});

//盤點格位內容
remainBagRouter.post('/queryLonoInfo', function (req, res) {
    const LONO = req.body.LONO;

    remainBagDB.queryLonoInfo(req.user, LONO)
        .then(val => {
            res.send(val);
        });
});

//更新格位內容
remainBagRouter.post('/updateLonoInfo', function (req, res) {
    const newProductNo = req.body.newProductNo;
    const newLotNo = req.body.newLotNo;
    const newWeight = req.body.newWeight;
    const opno = req.body.opno;
    const lono = req.body.lono;

    remainBagDB.updateLonoInfo(req.user, newProductNo, newLotNo, newWeight, opno, lono)
        .then(val => {
            res.send(val);
        });
});

//刪除格位內容
remainBagRouter.post('/deleteLonoInfo', function (req, res) {
    const lono = req.body.LONO;
    const opno = req.body.OPNO;
    const prd_pc = req.body.prd_pc;
    const lot_no = req.body.lot_no;
    const weight = req.body.weight;
    const doingType = req.body.doingType;

    remainBagDB.deleteLonoInfo(req.user, opno, lono, prd_pc, lot_no, weight, doingType)
        .then(val => {
            res.send(val);
        });
});

//品番(PRD_PC)格位
remainBagRouter.get('/getProductLono/:productNo', function (req, res) {
    const productNo = req.params.productNo;

    remainBagDB.getProductLono(req.user, productNo)
        .then(val => {
            res.send(val);
        });
});

export default remainBagRouter;
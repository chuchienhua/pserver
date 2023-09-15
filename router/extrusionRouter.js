import * as feedingDB from '../extrusion/oracleFeeding.js';
import * as extrusionDB from '../extrusion/oracleExtrusion.js';
import * as storageDB from '../extrusion/oracleStorage.js';
import * as formDB from '../extrusion/oracleForm.js';
import * as statisticsDB from '../extrusion/oracleStatistics.js';
import * as VisionTagsAPI from '../VisionTagsAPI.js';
import express from 'express';

const extrusionRouter = express.Router();

/* 押出入料 */
//取得每一條線正在處理或下一筆的工令
extrusionRouter.get('/getWorkingOrders', function (req, res) {
    feedingDB.getWorkingOrders(req.user)
        .then(val => res.send(val));
});

//取得押出入料管制表
extrusionRouter.get('/getFeedingForm/:line/:sequence', function (req, res) {
    const line = req.params.line;
    const sequence = req.params.sequence;

    feedingDB.getFeedingForm(line, sequence, req.user)
        .then(val => res.send(val));
});

//取得M1樹脂入料機"當下"的入料重量(到時候改為累計)，由Vision的Tags抓
extrusionRouter.post('/getSiloWeight/:line/:sequence', function (req, res) {
    const line = req.params.line;
    const sequence = req.params.sequence;
    const feeders = req.body.feeders; //[]查詢幾號入料機

    feedingDB.getSiloWeight(line, sequence, feeders, req.user)
        .then(val => res.send(val));
});

//儲存押出入料管制表
extrusionRouter.post('/createFeedingForm', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const materialArray = req.body.materialArray;

    feedingDB.createFeedingForm(line, sequence, materialArray, req.user)
        .then(val => res.send(val));
});

//更新押出入料/查核表的勾選項目
extrusionRouter.post('/updateFeedingForm', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const materialArray = req.body.materialArray;

    feedingDB.updateFeedingForm(line, sequence, materialArray, req.user)
        .then(val => res.send(val));
});

//刪除未入料過的管制表
extrusionRouter.post('/removeFeedingForm', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;

    feedingDB.removeFeedingForm(line, sequence, req.user)
        .then(val => res.send(val));
});

//押出入料扣帳
extrusionRouter.post('/powderfeeding', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const material = req.body.material;
    const feedNum = req.body.feedNum;
    const feedWeight = req.body.feedWeight;
    const feedLotNo = req.body.feedLotNo;
    const feedBatchNo = req.body.feedBatchNo;
    const semiNo = req.body.semiNo;

    feedingDB.powderfeeding(line, sequence, material, feedNum, feedWeight, feedLotNo, feedBatchNo, semiNo, req.user)
        .then(val => res.send(val));
});

//取得重工品料頭、前料、包裝棧板資訊
extrusionRouter.post('/reworkData/:reworkSource', function (req, res) {
    const reworkSource = req.params.reworkSource;
    const opno = req.body.opno;

    feedingDB.getReworkData(reworkSource, opno, req.user)
        .then(val => res.send(val));
});

//押出入料重工品
extrusionRouter.post('/reworkFeeding', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const reworkSource = req.body.reworkSource;
    const feederNo = req.body.feederNo;
    const reworkPickNum = req.body.reworkPickNum; //只有回爐品使用包裝棧板需要輸入數量
    const opno = req.body.opno;

    feedingDB.reworkFeeding(line, sequence, reworkSource, feederNo, reworkPickNum, opno, req.user)
        .then(val => res.send(val));
});

//殘包入料
extrusionRouter.post('/remainBagFeeding', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const opno = req.body.opno;
    const type = req.body.type; //重工rework、改番pick

    feedingDB.remainBagFeeding(line, sequence, opno, type, req.user)
        .then(val => res.send(val));
});

//入料機變更
extrusionRouter.post('/changeFeederNo', function (req, res) {
    const line = req.body.line;
    const sequence = req.body.sequence;
    const oldFeederNo = req.body.oldFeederNo;
    const newFeederNo = req.body.newFeederNo;
    const material = req.body.material;

    feedingDB.changeFeederNo(line, sequence, oldFeederNo, newFeederNo, material, req.user)
        .then(val => res.send(val));
});

/* 押出領繳部分 */
//押出領料繳庫量查詢API
extrusionRouter.post('/storage/:storageType/:queryType', function (req, res) {
    const storageType = req.params.storageType; //pay || picking || quality || palletPicking
    const queryType = req.params.queryType; //date || week || order || lotNo
    const date = req.body.date;
    const line = req.body.line;
    const seqStart = req.body.seqStart;
    const seqEnd = req.body.seqEnd;
    const lotNo = req.body.lotNo;

    if ('pay' === storageType) {
        storageDB.getInvtPay(queryType, date, line, seqStart, seqEnd, lotNo, req.user)
            .then(val => res.send(val));

    } else if ('picking' === storageType) {
        storageDB.getInvtPick(queryType, date, line, seqStart, seqEnd, lotNo, req.user)
            .then(val => res.send(val));

    } else if ('quality' === storageType) {
        if ('date' === queryType) {
            storageDB.getExtrusionQuality(date, queryType, req.user)
                .then(val => res.send(val));
        } else {
            storageDB.getOrderQuality(line, seqStart, seqEnd, lotNo, queryType, req.user)
                .then(val => res.send(val));
        }

    } else {
        res.send({ res: 'Frontend url error', error: true });
    }
});

//查詢批號領繳狀況
extrusionRouter.get('/invtDetail/:lotNo', function (req, res) {
    const lotNo = req.params.lotNo;

    storageDB.getLotNoInvtDetail(lotNo, req.user)
        .then(val => res.send(val));
});

//調整批號的領繳量
extrusionRouter.post('/adjustPickAndPay/:lotNo', function (req, res) {
    const lotNo = req.params.lotNo;
    const productNo = req.body.productNo;
    const rows = req.body.rows;

    storageDB.adjustPickAndPay(lotNo, productNo, rows, req.user)
        .then(val => res.send(val));
});

//建立回收料頭太空袋標籤
extrusionRouter.post('/createScrapBag', function (req, res) {
    const printer = req.body.printer;
    const bagSeries = req.body.bagSeries;

    extrusionDB.createScrapBag(printer, bagSeries, req.user)
        .then(val => res.send(val));
});

//查詢日期期間內所有已建立的太空袋
extrusionRouter.post('/getScrapBag/:startDate/:endDate', function (req, res) {
    const startDate = req.params.startDate;
    const endDate = req.params.endDate;
    const queryBagSeries = req.body.queryBagSeries;
    const lineSearch = req.body.lineSearch;
    const lotNoSearch = req.body.lotNoSearch;
    const seqSearch = req.body.seqSearch;
    const prdPCSearch = req.body.prdPCSearch;

    extrusionDB.getScrapBag(startDate, endDate, queryBagSeries, lineSearch, lotNoSearch, seqSearch, prdPCSearch, req.user)
        .then(val => res.send(val));
});

//取得回收料頭太空袋內的內容物
extrusionRouter.get('/getBagDetail/:batchNo', function (req, res) {
    const batchNo = req.params.batchNo;

    extrusionDB.getBagDetail(batchNo, req.user)
        .then(val => res.send(val));
});

//更新回收料頭太空袋內容物
extrusionRouter.post('/updateBags/:batchNo', function (req, res) {
    const batchNo = req.params.batchNo;
    const scrapList = req.body.scrapList;
    const type = req.body.type; //create加入料頭或update更新指定料頭重量(管理員)

    extrusionDB.updateBags(batchNo, scrapList, type, req.user)
        .then(val => res.send(val));
});

//移除空的回收料頭太空袋
extrusionRouter.get('/removeBag/:batchNo', function (req, res) {
    const batchNo = req.params.batchNo;

    extrusionDB.removeBags(batchNo, req.user)
        .then(val => res.send(val));
});

//列印粉碎料頭成品標籤
extrusionRouter.post('/printCrushScrap', function (req, res) {
    const printer = req.body.printer;
    const bagSeries = req.body.bagSeries;
    const weight = req.body.weight;

    extrusionDB.printCrushScrap(printer, bagSeries, weight, req.user)
        .then(val => res.send(val));
});

//列印前料標籤並記錄
extrusionRouter.post('/createHeadMaterial/:line/:sequence', function (req, res) {
    const line = req.params.line;
    const sequence = req.params.sequence;
    const productNo = req.body.productNo;
    const lotNo = req.body.lotNo;
    const weight = req.body.weight;
    const printer = req.body.printer;
    const prdReason = req.body.productreason;
    const remark = req.body.remark;

    extrusionDB.createHeadMaterial(printer, line, sequence, productNo, lotNo, weight, prdReason, remark, req.user)
        .then(val => res.send(val));
});

//查詢所有前料產出紀錄
extrusionRouter.post('/getHeadMaterial', function (req, res) {
    const startDate = req.body.startdate;
    const endDate = req.body.enddate;
    const lineSearch = req.body.lineSearch;
    const seqSearch = req.body.seqSearch;
    const productNoSearch = req.body.productNoSearch;
    const lotNoSearch = req.body.lotNoSearch;

    extrusionDB.getHeadMaterial(startDate, endDate, lineSearch, seqSearch, productNoSearch, lotNoSearch, req.user)
        .then(val => res.send(val));
});

/* 押出製造紀錄表 */
//產品簡碼與日期區間查詢
extrusionRouter.get('/getOrder/:line/:productNo/:startDate/:endDate', function (req, res) {
    const line = req.params.line;
    const productNo = req.params.productNo;
    const startDate = req.params.startDate;
    const endDate = req.params.endDate;

    formDB.getOrder(line, productNo, startDate, endDate, req.user)
        .then(val => res.send(val));
});

//查詢工令的押出製造紀錄/製程檢驗表
extrusionRouter.get('/getForm/:tableType/:line/:sequence', function (req, res) {
    const tableType = req.params.tableType;
    const line = req.params.line;
    const sequence = req.params.sequence;

    formDB.getForm(tableType, line, sequence, req.user)
        .then(val => res.send(val));
});

//儲存工令的一筆製造紀錄/製程檢驗表
extrusionRouter.post('/saveForm/:tableType/:line/:sequence', function (req, res) {
    const tableType = req.params.tableType;
    const line = req.params.line;
    const sequence = req.params.sequence;
    const stdArray = req.body.stdArray;

    formDB.saveForm(tableType, line, sequence, stdArray, req.user)
        .then(val => res.send(val));
});

//修改工令的一筆製造紀錄/製程檢驗表
extrusionRouter.post('/updateForm/:tableType/:line/:sequence', function (req, res) {
    const tableType = req.params.tableType;
    const line = req.params.line;
    const sequence = req.params.sequence;
    const stdArray = req.body.stdArray;

    formDB.updateForm(tableType, line, sequence, stdArray, req.user)
        .then(val => res.send(val));
});

//取得包裝SILO
extrusionRouter.get('/packingSilos', function (req, res) {
    formDB.getPackingSilo(req.user)
        .then(val => res.send(val));
});

//取得電錶抄錶紀錄
extrusionRouter.get('/getMeterRecord/:date/:workShift', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;

    formDB.getMeterRecord(date, workShift, req.user)
        .then(val => res.send(val));
});

//儲存/更新電錶抄錶紀錄
extrusionRouter.post('/saveMeterRecord/:date/:workShift', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;
    const recordArray = req.body.recordArray;

    formDB.saveMeterRecord(date, workShift, recordArray, req.user)
        .then(val => res.send(val));
});

//取得交接紀錄表
extrusionRouter.get('/getHandoverRecord/:date/:workShift', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;

    formDB.getHandoverRecord(date, workShift, req.user)
        .then(val => res.send(val));
});

//儲存/更新交接紀錄表
extrusionRouter.post('/saveHandoverRecord/:date/:workShift', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;
    const recordArray = req.body.recordArray;

    formDB.saveHandoverRecord(date, workShift, recordArray, req.user)
        .then(val => res.send(val));
});

//取得押出作業人員
extrusionRouter.get('/getOperator', function (req, res) {
    formDB.getExtrusionOperator(req.user)
        .then(val => res.send(val));
});

//取得交接紀錄停機異常原因
extrusionRouter.get('/getHandoverReason', function (req, res) {
    formDB.getHandoverReason(req.user)
        .then(val => res.send(val));
});

//取得日產量報表
extrusionRouter.get('/getDailyForm/:date/:workShift/:type', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;
    const type = req.params.type; //query一般查詢與update持續更新

    formDB.getDailyForm(date, workShift, type, req.user)
        .then(val => res.send(val));
});

//儲存一個班別的日產量報表
extrusionRouter.post('/saveDailyForm/:date/:workShift', function (req, res) {
    const date = req.params.date; //YYYYMMDD
    const workShift = req.params.workShift;
    const formArray = req.body.formArray;
    const handover = req.body.handover;
    const waterDeionized = req.body.waterDeionized;
    const waterWaste = req.body.waterWaste;
    const air = req.body.air;
    const getDataTime = req.body.getDataTime;

    formDB.saveDailyForm(date, workShift, formArray, handover, waterDeionized, waterWaste, air, getDataTime, req.user)
        .then(val => res.send(val));
});

//取得設備運轉統計值
extrusionRouter.get('/getExtruderStatistics/:line/:sequence/:productNo/:startDate/:endDate', function (req, res) {
    const line = req.params.line;
    const sequence = req.params.sequence;
    const productNo = req.params.productNo;
    const startDate = req.params.startDate; //YYYYMMDD
    const endDate = req.params.endDate; //YYYYMMDD

    statisticsDB.getExtruderStatistics(line, sequence, productNo, startDate, endDate, req.user)
        .then(val => res.send(val));
});

//取得線別與時間區間內的押出機Vision Tags電流與轉速值
extrusionRouter.post('/getExtruderTags', function (req, res) {
    const line = req.body.line;
    const startTime = req.body.startTime;
    const endTime = req.body.endTime;
    const filter = false; //要不要將Tag的異常值濾掉

    VisionTagsAPI.getStatisticsArray(line, startTime, endTime, filter, req.user)
        .then(val => res.send(val));
});

//停機項目分析
extrusionRouter.get('/getShutdown/:startDate/:endDate', function (req, res) {
    const startDate = req.params.startDate; //YYYYMMDD
    const endDate = req.params.endDate; //YYYYMMDD

    statisticsDB.getShutdown(startDate, endDate, req.user)
        .then(val => res.send(val));
});

//稼動率
extrusionRouter.get('/getLineAvability/:month', function (req, res) {
    const month = req.params.month; //YYYYMM

    formDB.getLineAvability(month, req.user)
        .then(val => res.send(val));
});

//個人績效
extrusionRouter.get('/getCrewPerformance/:startDate/:endDate', function (req, res) {
    const startDate = req.params.startDate; //YYYYMMDD
    const endDate = req.params.endDate; //YYYYMMDD

    formDB.getCrewPerformance(startDate, endDate, req.user)
        .then(val => res.send(val));
});

//生產總表
extrusionRouter.get('/getProductionSummary/:month', function (req, res) {
    const month = req.params.month; //YYYYMM

    formDB.getProductionSummary(month, req.user)
        .then(val => res.send(val));
});

//取得用電量統計相關報表
extrusionRouter.post('/getAmmeterStatistics', function (req, res) {
    const queryType = req.body.queryType;
    const searchDate = req.body.searchDate;
    const workShift = req.body.workShift;
    const line = req.body.line;
    const seqStart = req.body.seqStart;
    const seqEnd = req.body.seqEnd;
    const startDate = req.body.startDate;
    const endDate = req.body.endDate;

    if ('order' === queryType) {
        statisticsDB.getSchedulePowerConsumption(line, seqStart, seqEnd, req.user)
            .then(val => res.send(val));

    } else if ('date' === queryType) {
        statisticsDB.getDailyPowerConsumption(searchDate, workShift, req.user)
            .then(val => res.send(val));

    } else if ('stop' === queryType) {
        statisticsDB.getStopPowerConsumption(startDate, endDate, req.user)
            .then(val => res.send(val));

    } else {
        res.send({ res: 'Route Error', error: true });
    }
});

export default extrusionRouter;
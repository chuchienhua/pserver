import * as MixingDB from '../mixing/oracleMix.js';
import express from 'express';
import multer from 'multer';

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

const mixingRouter = express.Router();

/* 拌粉排程 */
//取得拌粉排程
mixingRouter.post('/getSchedule/:startDate/:endDate', function (req, res) {
    const startDate = req.params.startDate;
    const endDate = req.params.endDate;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const productNo = req.body.productNo;

    MixingDB.getMixingSchedule(startDate, endDate, line, sequence, productNo, req.user)
        .then(val => res.send(val));
});

//儲存單個原料領用排程
mixingRouter.post('/saveSchedule', function (req, res) {
    const date = req.body.date;
    const workShift = req.body.workShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const batchStart = req.body.batchStart;
    const batchEnd = req.body.batchEnd;
    const operator = req.body.operator;
    const semiNo = req.body.semiNo;
    const note = req.body.note;

    MixingDB.createMixingSchedule(date, workShift, line, sequence, semiNo, batchStart, batchEnd, operator, note, req.user)
        .then(val => res.send(val));
});

//移除單個原料領用排程
mixingRouter.post('/removeSchedule', function (req, res) {
    const date = req.body.date;
    const workShift = req.body.workShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const semiNo = req.body.semiNo;

    MixingDB.removeMixingSchedule(date, workShift, line, sequence, semiNo, req.user)
        .then(val => res.send(val));
});

//更新單個原料領用排程，(暫定)僅能更新Mixer
mixingRouter.post('/updateSchedule', function (req, res) {
    const date = req.body.date;
    const workShift = req.body.workShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const semiNo = req.body.semiNo;
    const mixer = req.body.mixer;
    const batchStart = req.body.batchStart;
    const batchEnd = req.body.batchEnd;
    const operator = req.body.operator;
    const note = req.body.note;

    MixingDB.updateMixingSchedule(date, workShift, line, sequence, semiNo, mixer, batchStart, batchEnd, operator, note, req.user)
        .then(val => res.send(val));
});

//取得拌粉原料標籤
mixingRouter.get('/labelMaterial/:date/:workShift/:line/:sequence/:semiNo', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;
    const line = req.params.line;
    const sequence = req.params.sequence;
    const semiNo = req.params.semiNo;

    MixingDB.getLabelMaterial(date, workShift, line, sequence, semiNo, req.user)
        .then(val => res.send(val));
});

//取得所有拌粉操作人員名單
mixingRouter.get('/mixOperator', function (req, res) {
    MixingDB.getMixOperator(req.user)
        .then(val => res.send(val));
});

//取得拌粉機清單
mixingRouter.get('/mixer', function (req, res) {
    MixingDB.getMixer(req.user)
        .then(val => res.send(val));
});

//列印拌粉原料標籤
mixingRouter.post('/printLabel', function (req, res) {
    const type = req.body.type;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const paperBagWeight = req.body.paperBagWeight;
    const date = req.body.date;
    const workShift = req.body.workShift;
    const batchStart = req.body.batchStart;
    const batchEnd = req.body.batchEnd;
    const materials = req.body.materials;
    const semiProductNo = req.body.semiProductNo;
    const semiProductWeight = req.body.semiProductWeight; //半成品總重
    const semiNum = req.body.semiNum; //包數
    const semiType = req.body.semiType; //包種
    const printerIP = req.body.printerIP;

    if ('normal' === type || 'pallet' === type || 'semi' === type) {
        MixingDB.printLabel(
            type, line, sequence, paperBagWeight, date, workShift, batchStart, batchEnd,
            materials, printerIP, semiProductNo, semiProductWeight, semiNum, semiType, req.user
        ).then(val => res.send(val));
    } else if ('powder' === type) {
        //僅列印原料標籤，不列印半成品標籤
        MixingDB.printLabel(
            type, line, sequence, paperBagWeight, date, workShift, batchStart, batchEnd,
            materials, printerIP, null, null, null, null, req.user
        ).then(val => res.send(val));
    } else {
        res.send({ res: 'LabelType Error', error: true });
    }
});

/* 拌粉PDA */
//取得拌粉原料領料
mixingRouter.get('/pickingMaterial/:date/:workShift', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;

    MixingDB.getPickingMaterial(date, workShift, req.user)
        .then(val => res.send(val));
});

//拌粉PDA領料QRCode掃碼後紀錄line, sequence, productNo, rows[row].MATERIAL, value
mixingRouter.post('/pdaPicking', function (req, res) {
    const pickDate = req.body.pickDate;
    const pickShift = req.body.pickShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const batchStart = req.body.batchStart;
    const batchEnd = req.body.batchEnd;
    const semiNo = req.body.semiNo;
    const material = req.body.material;
    const pickLotNo = req.body.pickLotNo; //LotNo
    const pickBatchNo = req.body.pickBatchNo; //棧板編號
    const bagPickWeight = req.body.bagPickWeight; //棧板領用總重
    const bagPickNum = req.body.bagPickNum; //棧板領用包數
    const remainderPickWeight = req.body.remainderPickWeight; //餘料領用重量
    const totalNeedWeight = req.body.totalNeedWeight; //需求量

    console.log(pickDate, pickShift, line, sequence, batchStart, batchEnd, semiNo, material, pickLotNo, pickBatchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight);
    MixingDB.pdaPicking(pickDate, pickShift, line, sequence, batchStart, batchEnd, semiNo, material, pickLotNo, pickBatchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight, req.user)
        .then(val => res.send(val));
});

//拌粉領料全數完成後扣帳
mixingRouter.post('/pickingDeduct', function (req, res) {
    const pickDate = req.body.pickDate;
    const pickShift = req.body.pickShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const semiNo = req.body.semiNo;
    const deductArray = req.body.deductArray;

    MixingDB.pickingDeduct(pickDate, pickShift, line, sequence, semiNo, deductArray, req.user)
        .then(val => res.send(val));
});

//取得拌粉備料
mixingRouter.get('/stockMixing/:date/:workShift', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;

    MixingDB.getStockMixing(date, workShift, req.user)
        .then(val => res.send(val));
});

//取得拌粉備料的原料重量
mixingRouter.get('/stockMixingMaterial/:date/:workShift/:line/:sequence/:semiNo/:batchSequence', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;
    const line = req.params.line;
    const sequence = req.params.sequence;
    const semiNo = req.params.semiNo;
    const batchSequence = req.params.batchSequence;

    MixingDB.getStockMixingMaterial(date, workShift, line, sequence, semiNo, batchSequence, req.user)
        .then(val => res.send(val));
});

//拍照確認備料
mixingRouter.post('/stockEnsure/:stockDate/:stockShift/:line/:sequence/:semiNo/:batch', upload.single('image'), function (req, res) {
    //const image = req.body.image.toString('hex');
    //const image = req.file.buffer.toString('base64');
    const stockDate = req.params.stockDate;
    const stockShift = req.params.stockShift;
    const line = req.params.line;
    const sequence = req.params.sequence;
    const semiNo = req.params.semiNo;
    const batch = req.params.batch;

    const image = req.file.buffer;

    MixingDB.stockEnsure(image, stockDate, stockShift, line, sequence, semiNo, batch, req.user)
        .then(val => res.send(val));
});

//取得拌料確認的照片
mixingRouter.get('/getEnsureImage/:line/:sequence/:date/:workShift', function (req, res) {
    const line = req.params.line;
    const sequence = req.params.sequence;
    const date = req.params.date;
    const workShift = req.params.workShift;

    MixingDB.getEnsureImage(line, sequence, date, workShift, req.user)
        .then(val => res.send(val));
});

//取得排程入料狀況
mixingRouter.get('/feedStatus/:date/:workShift', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;

    MixingDB.getFeedStatus(date, workShift, req.user)
        .then(val => res.send(val));
});

//拌粉PDA入料QRCode掃碼後紀錄line, sequence, productNo, rows[row].MATERIAL, value
mixingRouter.post('/pdaFeeding/:type', function (req, res) {
    const type = req.params.type;
    const feedDate = req.body.feedDate;
    const feedShift = req.body.feedShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const material = req.body.material;
    const batch = req.body.batch;

    console.log(feedDate, feedShift, line, sequence, material, batch);
    MixingDB.pdaFeeding(type, feedDate, feedShift, line, sequence, material, batch, req.user)
        .then(val => res.send(val));
});

//漳州用，同時查詢領用量與入料狀況
mixingRouter.get('/mixWorkStatus/:date/:workShift', function (req, res) {
    const date = req.params.date;
    const workShift = req.params.workShift;

    MixingDB.getMixWorkStatus(date, workShift, req.user)
        .then(val => res.send(val));
});

//漳州用，PDA掃描QRCode後直接做領料+入料
mixingRouter.post('/pickAndFeed', function (req, res) {
    const feedDate = req.body.feedDate;
    const feedShift = req.body.feedShift;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const batch = req.body.batch;
    const semiNo = req.body.semiNo;
    const material = req.body.material;
    const lotNo = req.body.pickLotNo; //棧板LotNo
    const batchNo = req.body.pickBatchNo; //棧板編號
    const bagPickWeight = req.body.bagPickWeight; //棧板領用總重
    const bagPickNum = req.body.bagPickNum; //棧板領用包數
    const remainderPickWeight = req.body.remainderPickWeight; //餘料領用重量
    const totalNeedWeight = req.body.totalNeedWeight; //需求量

    console.log(feedDate, feedShift, line, sequence, batch, semiNo, material, lotNo, batchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight);
    MixingDB.pickAndFeed(feedDate, feedShift, line, sequence, batch, semiNo, material, lotNo, batchNo, bagPickWeight, bagPickNum, remainderPickWeight, totalNeedWeight, req.user)
        .then(val => res.send(val));
});

export default mixingRouter;
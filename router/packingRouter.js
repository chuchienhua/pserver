import * as PackingData from '../packing/oraclePacking.js';
import * as PackingWork from '../packing/packingWork.js';
import * as PackingReport from '../packing/packingReport.js';
import { getNowDatetimeString } from '../libs.js';
import express from 'express';
import multer from 'multer';
const upload = multer();

const packingRouter = express.Router();

//頁面基本資料
packingRouter.get('/options', function (req, res) {
    const timeName = `${getNowDatetimeString()} getPackingOptions_${req.user.PPS_CODE}_${Math.random().toString(16).slice(2, 10)}`;
    console.time(timeName);
    PackingData.getPackingOptions(req.user)
        .then(val => {
            console.timeEnd(timeName);
            res.send(val);
        });
});

//查詢包裝排程
packingRouter.post('/getPackingSchedule', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    const queryMode = req.body.queryMode;
    const proLine = req.body.proLine;
    const proSeq = req.body.proSeq;
    PackingData.getPackingSchedule(req.user, packingDateStart, packingDateEnd, queryMode, proLine, proSeq)
        .then(val => {
            res.send(val);
        });
});

//儲存包裝排程
packingRouter.post('/savePackingSchedule', function (req, res) {
    const rows = req.body.rows;
    PackingData.savePackingSchedule(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//刪除包裝排程
packingRouter.post('/deletePackingSchedule', function (req, res) {
    const rows = req.body.rows;
    PackingData.deletePackingSchedule(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝項次
packingRouter.post('/getPackingDetail', function (req, res) {
    const packingSeq = req.body.packingSeq;
    PackingData.getPackingDetail(req.user, packingSeq)
        .then(val => {
            res.send(val);
        });
});

//儲存包裝項次
packingRouter.post('/savePackingDetail', function (req, res) {
    const rows = req.body.rows;
    PackingData.savePackingDetail(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//取得棧板編號
packingRouter.post('/getPackingPalletNo', function (req, res) {
    PackingWork.getPalletNo(req.user)
        .then(val => {
            res.send(val);
        });
});

//列印標籤
packingRouter.post('/printLabel', function (req, res) {
    const scheduleData = req.body.scheduleData;
    const detailData = req.body.detailData;
    const printerData = req.body.printerData;
    PackingWork.printLabel(req.user, scheduleData, detailData, printerData)
        .then(val => {
            res.send(val);
        });
});

//上傳包裝照片
packingRouter.post('/uploadImage', upload.single('image'), function (req, res) {
    const detailData = req.body.detailData;
    const file = req.file;
    PackingWork.uploadImage(req.user, detailData, file)
        .then(val => {
            res.send(val);
        });
});

//列印殘包標籤
packingRouter.post('/printRemainderLabel', function (req, res) {
    const scheduleData = req.body.scheduleData;
    const printerData = req.body.printerData;
    PackingWork.printRemainderLabel(req.user, scheduleData, printerData)
        .then(val => {
            res.send(val);
        });
});

//包裝結束
packingRouter.post('/finishPacking', function (req, res) {
    const scheduleData = req.body.scheduleData;
    PackingWork.finishPacking(req.user, scheduleData)
        .then(val => {
            res.send(val);
        });
});

//查詢生產排程
packingRouter.post('/getProScheduleByProductionLine', function (req, res) {
    const LINE = req.body.LINE;
    const SEQ = req.body.SEQ;
    PackingData.getProScheduleByProductionLine(req.user, LINE, SEQ)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝日報表
packingRouter.post('/getPackingDailyReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    PackingReport.getPackingDailyReport(req.user, packingDateStart, packingDateEnd)
        .then(val => {
            res.send(val);
        });
});

//儲存每日包裝出勤及槽車灌充作業
packingRouter.post('/savePackingDailyAttendanceReport', function (req, res) {
    const rows = req.body.rows;
    PackingReport.savePackingDailyAttendanceReport(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//儲存每日包裝明細表
packingRouter.post('/savePackingDailyDetailReport', function (req, res) {
    const rows = req.body.rows;
    PackingReport.savePackingDailyDetailReport(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝統計表
packingRouter.post('/getPackingStatReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    const isGroupBySchedule = true;
    PackingReport.getPackingStatReport(req.user, packingDateStart, packingDateEnd, isGroupBySchedule)
        .then(val => {
            res.send(val);
        });
});

//查詢排程達成率統計表
packingRouter.post('/getPackingCompletionRateReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    PackingReport.getPackingCompletionRateReport(req.user, packingDateStart, packingDateEnd)
        .then(val => {
            res.send(val);
        });
});

//儲存排程達成率統計表
packingRouter.post('/savePackingCompletionRateReport', function (req, res) {
    const rows = req.body.rows;
    PackingReport.savePackingCompletionRateReport(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝量統計表
packingRouter.post('/getPackingQuantityStatReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    const isGroupBySchedule = false;
    PackingReport.getPackingStatReport(req.user, packingDateStart, packingDateEnd, isGroupBySchedule)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝費用統計表
packingRouter.post('/getPackingExpenseStatReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    PackingReport.getPackingExpenseStatReport(req.user, packingDateStart, packingDateEnd)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝個人績效表
packingRouter.post('/getPackingPerformanceReport', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    const packingDateEnd = req.body.packingDateEnd;
    PackingReport.getPackingPerformanceReport(req.user, packingDateStart, packingDateEnd)
        .then(val => {
            res.send(val);
        });
});

//查詢包裝項目單價表
packingRouter.post('/getPackingExpenseItemsReport', function (req, res) {
    PackingReport.getPackingExpenseItemsReport(req.user)
        .then(val => {
            res.send(val);
        });
});

//儲存包裝項目單價表
packingRouter.post('/savePackingExpenseItemsReport', function (req, res) {
    const rows = req.body.rows;
    PackingReport.savePackingExpenseItemsReport(req.user, rows)
        .then(val => {
            res.send(val);
        });
});

//結束特定日期的包裝排程，並自動產生未完成的排程到隔天
packingRouter.post('/finishPackingScheduleByDay', function (req, res) {
    const packingDateStart = req.body.packingDateStart;
    PackingData.finishPackingScheduleByDay(req.user, packingDateStart, packingDateStart)
        .then(val => {
            res.send(val);
        });
});

export default packingRouter;
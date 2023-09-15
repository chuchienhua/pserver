import * as extrusionSummaryDB from '../extrusion/oracleSummaryReport.js';
import express from 'express';

const extrusionSummaryRouter = express.Router();

//生產日報
extrusionSummaryRouter.post('/getDayReport', function (req, res) {
    const date = req.body.targetDate;

    extrusionSummaryDB.getDayReport(req.user, date)
        .then(val => {
            res.send(val);
        });
});

//生產月報
extrusionSummaryRouter.post('/getMonthReport', function(req, res) {
    const startDate = req.body.targetStartOfMonth;
    const endDate = req.body.targetEndOfMonth;

    extrusionSummaryDB.getMonthReport(req.user, startDate, endDate)
        .then(val => {
            res.send(val);
        });
});

//匯出生產日報
extrusionSummaryRouter.post('/exportDayReport', function (req, res) {
    const data = req.body.rows;

    extrusionSummaryDB.exportDayRport(data)
        .then(val => {
            res.send(val);
        });
});

//匯出生產月報
extrusionSummaryRouter.post('/exportMonthReport', function (req, res) {
    const data = req.body.rows;

    extrusionSummaryDB.exportMonthRport(data)
        .then(val => {
            res.send(val);
        });
});

export default extrusionSummaryRouter;
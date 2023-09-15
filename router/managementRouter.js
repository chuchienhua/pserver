import * as ReportData from '../management/oracleManagement.js';
import express from 'express';

const managementRouter = express.Router();

//查詢原料庫存追蹤表
managementRouter.post('/getMaterialInvTraceReport', function (req, res) {
    const reportDate = req.body.reportDate;
    ReportData.getMaterialInvTraceReport(req.user, reportDate)
        .then(val => {
            res.send(val);
        });
});

export default managementRouter;
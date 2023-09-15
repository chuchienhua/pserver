import * as traceDB from '../trace/oracleTrace.js';
import express from 'express';

const traceRouter = express.Router();

/* 生產追溯功能 */
//包裝棧板查詢使用的原料
traceRouter.post('/materials', function (req, res) {
    const queryType = req.body.queryType;
    const packPalletNo = req.body.packPalletNo;
    const lotNo = req.body.lotNo;
    const productNo = req.body.productNo;
    const startDate = req.body.startDate;
    const endDate = req.body.endDate;

    if ('packPallet' === queryType) {
        traceDB.getMaterials(packPalletNo, req.user)
            .then(val => res.send(val));

    } else {
        traceDB.getMaterialsAudit(queryType, lotNo, productNo, startDate, endDate, req.user)
            .then(val => res.send(val));
    }
});

//原料品名/LOTNO追溯包裝棧板
traceRouter.get('/packPalletNo/:queryType/:lotNo', function (req, res) {
    const queryType = req.params.queryType; //powder或silo
    const lotNo = req.params.lotNo;

    traceDB.getPackPalletNo(queryType, lotNo, req.user)
        .then(val => res.send(val));
});

//工令查詢所有棧板使用紀錄
traceRouter.post('/palletPicked', function (req, res) {
    const queryType = req.body.queryType;
    const searchDate = req.body.searchDate;
    const line = req.body.line;
    const sequence = req.body.sequence;
    const lotNo = req.body.lotNo;
    const productNo = req.body.productNo;
    const startDate = req.body.startDate;
    const endDate = req.body.endDate;
    const pickStage = req.body.pickStage;

    traceDB.getPalletPicked(queryType, searchDate, line, sequence, lotNo, productNo, startDate, endDate, pickStage, req.user)
        .then(val => res.send(val));
});

export default traceRouter;
import * as siloDB from '../siloStatus/oracleSiloStatus.js';
import express from 'express';

const siloRouter = express.Router();

/* SILO看板 */
//取得所有SILO與目前儲位狀況
siloRouter.get('/getSilos', function (req, res) {
    siloDB.getSilos(req.user)
        .then(val => res.send(val));
});

//取得正在使用中的SILO
siloRouter.get('/getUsingSilos', function (req, res) {
    siloDB.getUsingSilos(req.user)
        .then(val => res.send(val));
});

export default siloRouter;
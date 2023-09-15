import config from './config.js';
import axios from 'axios';
import oracledb from 'oracledb';
import moment from 'moment';
import { getNowDatetimeString } from './libs.js';
import { mongoDbName, connectToMongo, MongoClientReconnect } from './mongodb.js';

//豪哥提供Vision下的Tags
const VISION_TAGS_API = 'http://vision.ccpgp.com/api/tags/readTagValueFromOPC';

//由線別抓所需的TAGS
export const mapOpcTags = async (line, user) => {
    let tagsData = {
        connector_name: 'KH',
        opc_ip: '192.168.160.75',
        topic: 'OPC/KH/P2/PT2',
        feederTopic: 'OPC/KH/P2/PT2', //漳州廠才有分押出機/入料機
        siloTags: '', //正在使用幾號SILO
        feederTags: [], //M1-M7當前的入料量
        rpmTags: '', //押出機轉速
        ecTags: '', //押出機電流or負載
        temperatureTags: [], //押出機溫度
        dieTags: [], //押出模頭溫度
        ammeterTags: '', //電表讀數
        powerTags: '', //累計用電量
    };

    if ('A' === user.COMPANY) {
        tagsData.connector_name = 'FJ';
        tagsData.opc_ip = '';
        tagsData.topic = 'PLC/FJ/PBT/P1';
        tagsData.feederTopic = 'PLC/FJ/PBT/P1_FEEDER';
    }

    let conn;
    try {
        conn = await oracledb.getConnection(config.ORACLE_CONFIG);
        const sql = `
            SELECT TAGS_SILO, TAGS_FEEDER, TAGS_RPM, TAGS_EC, TAGS_TEMPERATURE, TAGS_DIE, TAGS_AMMETER, TAGS_POWER
            FROM PBTC_IOT_VISION_TAGS
            WHERE LINE = :LINE
            AND COMPANY = :COMPANY
            AND FIRM = :FIRM `;
        const params = {
            LINE: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: line.toString() },
            COMPANY: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.COMPANY },
            FIRM: { dir: oracledb.BIND_IN, type: oracledb.STRING, val: '' + user.FIRM },
        };
        const options = { outFormat: oracledb.OBJECT };
        const result = await conn.execute(sql, params, options);
        result.rows.forEach(element => {
            tagsData.siloTags = element.TAGS_SILO;
            tagsData.feederTags = element.TAGS_FEEDER.split(',');
            tagsData.rpmTags = element.TAGS_RPM;
            tagsData.ecTags = element.TAGS_EC;
            tagsData.temperatureTags = (element.TAGS_TEMPERATURE) ? element.TAGS_TEMPERATURE.split(',') : []; //有些線沒有溫度的Tags
            tagsData.dieTags = (element.TAGS_DIE) ? element.TAGS_DIE.split(',') : [];
            tagsData.ammeterTags = element.TAGS_AMMETER;
            tagsData.powerTags = element.TAGS_POWER;
        });
    } catch (err) {
        console.error(getNowDatetimeString(), 'mapOpcTags', err);
    } finally {
        if (conn) {
            await conn.close();
        }
    }

    return tagsData;
};

//取得線別目前所使用的SILO，只有高雄廠OPC用此功能
export const getLineUsingSilo = async (line, user) => {
    const tagsData = await mapOpcTags(line, user);
    let bodyData = {
        connector_name: tagsData.connector_name,
        opc_ip: tagsData.opc_ip,
        tags: [tagsData.siloTags, tagsData.feederTags[0]],
        project: 'PBTC-IOT',
    };

    return axios.post(VISION_TAGS_API, bodyData, { proxy: false });
};

//取得線別目前電表讀數
export const getLineAmmeter = async (line, user) => {
    let obj = {
        res: null,
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);
        if (tagsData.ammeterTags) {
            let bodyData = {
                connector_name: tagsData.connector_name,
                opc_ip: tagsData.opc_ip,
                tags: [tagsData.ammeterTags],
                project: 'PBTC-IOT',
            };
            const apiResult = await axios.post(VISION_TAGS_API, bodyData, { proxy: false });
            if (!apiResult.data.error) {
                obj.res = Object.values(apiResult.data.tags)[0];
            }
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getLineAmmeter', err);
        obj.error = true;
    }

    return obj;
};

//取得工令下各個入料機的累計入料量
export const getAccumulateWeight = async (line, startTime, endTime, timeAdjust, user) => {
    let obj = {
        res: [],
        error: false,
        feederFound: 0,
    };

    try {
        const tagsData = await mapOpcTags(line, user);
        obj.feederFound = tagsData.feederTags.length;

        let mongoClient = await connectToMongo();
        const db = mongoClient.db(mongoDbName);
        const colDatasource = db.collection('TimeTagsList@MIN');

        //STEP1: 先確認起始與結束時間是否有值，有值直接相減
        let matchCondition = {};
        matchCondition['_id.Topic'] = tagsData.feederTopic;

        if (timeAdjust) {
            startTime = moment(startTime).subtract(10, 'm').set({ second: 0, millisecond: 0 }).toDate();
            endTime = (endTime) ? moment(endTime).add(10, 'm').set({ second: 0, millisecond: 0 }).toDate() : moment().set({ second: 0, millisecond: 0 }).toDate();
        } else {
            startTime = moment(startTime).set({ second: 0, millisecond: 0 }).toDate();
            endTime = (endTime) ? moment(endTime).subtract(1, 'minute').set({ second: 0, millisecond: 0 }).toDate() : moment().subtract(1, 'minute').set({ second: 0, millisecond: 0 }).toDate();
        }

        //允許斷線時間半小時
        matchCondition['$or'] = [
            { '_id.Time': { '$gte': startTime, '$lt': moment(startTime).add(5, 'minutes').toDate() } },
            { '_id.Time': { '$gte': moment(endTime).subtract(5, 'minutes').toDate(), '$lt': endTime } },
        ];
        //matchCondition['_id.Time'] = { '$in': [startTime, endTime] };

        //切換廠別的ComeFrom Index，加速查詢時間
        if ('1' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
        } else if ('A' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
        }

        let sortCondition = {};
        sortCondition['_id.Time'] = 1;

        let groupCondition = {};
        groupCondition['_id'] = '$_id.Topic';
        tagsData.feederTags.forEach((feederTag, index) => {
            /*
            groupCondition[`M${index + 1}_maxWeight`] = { '$max': `$TagsSummary.${feederTag}.avg` };
            groupCondition[`M${index + 1}_minWeight`] = { '$min': `$TagsSummary.${feederTag}.avg` };
            */
            groupCondition[`M${index + 1}_maxWeight`] = { '$last': `$TagsSummary.${feederTag}.avg` };
            groupCondition[`M${index + 1}_minWeight`] = { '$first': `$TagsSummary.${feederTag}.avg` };
        });

        let result = await colDatasource.aggregate([
            { '$match': matchCondition },
            { '$sort': sortCondition },
            { '$group': groupCondition },
        ]).limit(1).toArray();
        obj.res = result[0];
    } catch (err) {
        console.error(getNowDatetimeString(), 'getAccumulateWeight', err);
        obj.error = true;
        MongoClientReconnect();
    }

    return obj;
};

//取得當下押出機的溫度、轉速、負載
export const getExtruderData = async (line, user) => {
    const tagsData = await mapOpcTags(line, user);
    let bodyData = {
        connector_name: tagsData.connector_name,
        opc_ip: tagsData.opc_ip,
        tags: [tagsData.rpmTags, tagsData.ecTags, tagsData.dieTags, tagsData.temperatureTags].flat(),
        project: 'PBTC-IOT',
    };

    return axios.post(VISION_TAGS_API, bodyData, { proxy: false });
};

//取得特定時間(班別結束)的所有押出機狀況
export const getExtrusionStatus = async (line, endTime, user) => {
    let obj = {
        res: 0,
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);

        let mongoClient = await connectToMongo();
        const db = mongoClient.db(mongoDbName);
        const colDatasource = db.collection('TimeTagsList@MIN');

        let matchCondition = {};
        matchCondition['_id.Topic'] = tagsData.topic;
        matchCondition['_id.Time'] = { '$gte': moment(endTime).subtract(5, 'minute').toDate(), '$lte': endTime };

        if ('1' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
        } else if ('A' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
        }

        let projectCondition = {};
        projectCondition['_id'] = 0;
        projectCondition[`TagsSummary.${tagsData.ecTags}`] = 1;

        let result = await colDatasource.aggregate([
            { '$match': matchCondition },
            { '$project': projectCondition },
        ]).limit(1).toArray();
        if (result.length) {
            obj.res = result[0]['TagsSummary'][tagsData.ecTags]['avg'];
        } else {
            throw new Error(`${line}線押出機負載Tags異常`);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getExtrusionStatus', err);
        obj.error = true;
    }

    return obj;
};

//線別從工令開始到該班別結束的生產經時，單位hr
export const getProductionTime = async (line, startTime, endTime, user) => {
    let obj = {
        productionTime: 0,
        stopTime: (endTime - startTime) / (60 * 60 * 1000),
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);

        let mongoClient = await connectToMongo();
        const db = mongoClient.db(mongoDbName);
        const colDatasource = db.collection('TimeTagsList@MIN');

        let matchIndexCondition = {};
        matchIndexCondition['_id.Topic'] = tagsData.topic;
        matchIndexCondition['_id.Time'] = { '$gte': startTime, '$lt': endTime };

        if ('1' === user.COMPANY) {
            matchIndexCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
        } else if ('A' === user.COMPANY) {
            matchIndexCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
        }

        let matchCondition = {};
        matchCondition[`TagsSummary.${tagsData.ecTags}.avg`] = { '$gte': 20 }; //押出機轉速高於20代表啟動

        let result = await colDatasource.aggregate([
            { '$match': matchIndexCondition },
            { '$match': matchCondition },
            { '$count': 'tagsCount' },
        ]).limit(1).toArray();
        if (result.length) {
            obj.productionTime = result[0]['tagsCount'] ? result[0]['tagsCount'] / 60 : 0;
            obj.stopTime = ((endTime - startTime) / (60 * 60 * 1000) >= obj.productionTime) ? ((endTime - startTime) / (60 * 60 * 1000) - obj.productionTime) : 0;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getProductionTime', err);
        obj.error = true;
    }

    return obj;
};

//取得工令時間內的所有電流/負載Tags值
export const getStatisticsArray = async (line, startTime, endTime, filter, user) => {
    let obj = {
        ec: [],
        rpm: [],
        time: [],
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);

        let mongoClient = await connectToMongo();
        const db = mongoClient.db(mongoDbName);
        const colDatasource = db.collection('TimeTagsList@MIN');

        let matchIndexCondition = {};
        matchIndexCondition['_id.Topic'] = tagsData.topic;
        matchIndexCondition['_id.Time'] = { '$gte': new Date(startTime), '$lt': new Date(endTime) };

        if ('1' === user.COMPANY) {
            matchIndexCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
        } else if ('A' === user.COMPANY) {
            matchIndexCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
        }

        let matchCondition = {};
        if (filter) {
            matchCondition[`TagsSummary.${tagsData.ecTags}.avg`] = { '$gte': 20, '$lt': 10000 };
            matchCondition[`TagsSummary.${tagsData.rpmTags}.avg`] = { '$gte': 20, '$lt': 10000 };
        } else {
            //關機不用濾掉，但通訊異常>10000還是先濾掉
            matchCondition[`TagsSummary.${tagsData.ecTags}.avg`] = { '$lt': 10000 };
            matchCondition[`TagsSummary.${tagsData.rpmTags}.avg`] = { '$lt': 10000 };
        }

        let projectCondition = {};
        projectCondition['_id'] = 1;
        projectCondition['ec'] = `$TagsSummary.${tagsData.ecTags}.avg`;
        projectCondition['rpm'] = `$TagsSummary.${tagsData.rpmTags}.avg`;

        let sortCondition = {};
        sortCondition['_id.Time'] = 1;

        let result = await colDatasource.aggregate([
            { '$match': matchIndexCondition },
            { '$match': matchCondition },
            { '$project': projectCondition },
            { '$sort': sortCondition },
        ]).toArray();

        if (result.length) {
            obj.ec = result.map(x => x.ec);
            obj.rpm = result.map(x => x.rpm);
            obj.time = result.map(x => x._id.Time);
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getStatisticsArray', err);
        obj.error = true;
    }

    return obj;
};

//線別某時間區間的用電量
export const getPowerConsumption = async (line, startTime, endTime, user) => {
    let obj = {
        res: 0,
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);

        if (tagsData.powerTags) {
            let mongoClient = await connectToMongo();
            const db = mongoClient.db(mongoDbName);
            const colDatasource = db.collection('TimeTagsList@MIN');

            let matchCondition = {};
            matchCondition['_id.Topic'] = tagsData.feederTopic;

            startTime = moment(startTime).set({ second: 0, millisecond: 0 }).toDate();
            endTime = (endTime) ? moment(endTime).set({ second: 0, millisecond: 0 }).toDate() : moment().subtract(1, 'minute').set({ second: 0, millisecond: 0 }).toDate();

            //允許斷線時間
            matchCondition['$or'] = [
                { '_id.Time': { '$gte': startTime, '$lt': moment(startTime).add(5, 'minutes').toDate() } },
                { '_id.Time': { '$gte': moment(endTime).subtract(5, 'minutes').toDate(), '$lte': endTime } },
            ];

            if ('1' === user.COMPANY) {
                matchCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
            } else if ('A' === user.COMPANY) {
                matchCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
            }

            let sortCondition = {};
            sortCondition['_id.Time'] = 1;

            let groupCondition = {};
            groupCondition['_id'] = '$_id.Topic';
            groupCondition['MAX_POWER'] = { '$last': `$TagsSummary.${tagsData.powerTags}.avg` };
            groupCondition['MIN_POWER'] = { '$first': `$TagsSummary.${tagsData.powerTags}.avg` };

            let result = await colDatasource.aggregate([
                { '$match': matchCondition },
                { '$sort': sortCondition },
                { '$group': groupCondition },
            ]).limit(1).toArray();
            obj.res = result[0];
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPowerConsumption', err);
        obj.error = true;
    }

    return obj;
};

/**
 * 依照電流Tags計算工令實際開關機時間
 * @param {string} line 工令線別
 * @param {string} type 找開始"START"或結束"END"時間
 * @param {Date} time 工令於主排程輸入之開始或結束時間
 * @param {number} allowErrorTime 允許誤差時間
 * @param {object} user 公司廠別
 * @returns 
 */
export const getScheduleActualTime = async (line, type, time, allowErrorTime, user) => {
    let obj = {
        res: [],
        error: false,
    };

    try {
        const tagsData = await mapOpcTags(line, user);

        let mongoClient = await connectToMongo();
        const db = mongoClient.db(mongoDbName);
        const colDatasource = db.collection('TimeTagsList@MIN');

        //根據允許誤差時間計算時間區間
        const allowStartTime = ('START' === type) ? moment(time).subtract(allowErrorTime, 'minutes').toDate() : time;
        const allowEndTime = ('START' === type) ? time : moment(time).add(allowErrorTime, 'minutes').toDate();

        //在這個時間區間尋找"電流>20"的第一筆時間點
        let matchCondition = {};
        matchCondition['_id.Topic'] = tagsData.feederTopic;
        matchCondition['_id.Time'] = { '$gte': allowStartTime, '$lte': allowEndTime };
        matchCondition[`TagsSummary.${tagsData.ecTags}.avg`] = { '$gte': 20 }; //押出機電流高於20表示開機，FIXME:K線電流不穩定、N線恆0

        //切換廠別的ComeFrom Index，加速查詢時間
        if ('1' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 1060000000000000, '$lt': 1070000000000000 };
        } else if ('A' === user.COMPANY) {
            matchCondition['ComeFrom'] = { '$gte': 2010000000000000, '$lt': 2019999999999999 };
        }

        let sortCondition = {};
        sortCondition['_id.Time'] = ('START' === type) ? 1 : -1;

        let groupCondition = {};
        groupCondition['_id'] = '$_id.Topic';
        groupCondition['VISION_TIME'] = { '$first': '$_id.Time' };

        let result = await colDatasource.aggregate([
            { '$match': matchCondition },
            { '$sort': sortCondition },
            { '$group': groupCondition },
        ]).limit(1).toArray();
        obj.res = result;
    } catch (err) {
        console.error(getNowDatetimeString(), 'getScheduleActualTime', err);
        obj.error = true;
        MongoClientReconnect();
    }

    return obj;
};
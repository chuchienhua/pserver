import { getNowDatetimeString } from './libs.js';

/* 使用CCP_IOT_API程式 */
//預設的連線字串
let MONGO_URI = 'mongodb://ccpVision:sldifuhvbalwi@192.168.8.242:27018,192.168.8.242:27019,192.168.8.242:27020/?readPreference=secondaryPreferred&authSource=admin&authMechanism=SCRAM-SHA-256&w=majority&readConcernLevel=majority';

//從環境變數中讀取設定
if (('string' === typeof process.env.MONGO_URI) && process.env.MONGO_URI) {
    //console.log(process.env.MONGO_URI);
    MONGO_URI = process.env.MONGO_URI;
}

export function createMongoURI(db) {
    //這邊假設帶進來的MONGO_URI中間不能有db name，字串中必須包含/?
    const [host, params] = MONGO_URI.split('/?');
    if ('undefined' === typeof params) {
        console.error('MONGO_URI格式錯誤，不可出現DB Name且必須包含/?字串');
    }
    return `${host}/${db}?${params}`;
}

import Mongo from 'mongodb';
export { Mongo };
const MongoClient = Mongo.MongoClient;

//Mongo Database Name
export const mongoDbName = 'ccpvisiondb';
const mongodbUrl = createMongoURI(mongoDbName);
//const assert = require('assert');

let globalClient = null; //最新的MongoClient

//mongoDB建立連線方式修改，已建立不再重複建
export async function connectToMongo() {
    try {
        if (globalClient) {
            if (globalClient instanceof Mongo.MongoClient) {
                await globalClient.connect();
            }
            return globalClient;
        } else {
            const client = await MongoClient.connect(mongodbUrl, {
                writeConcern: { w: 'majority' },
            }).catch(err => {
                throw err;
            });
            //連線建立成功
            console.log(`${getNowDatetimeString()} MongoClient created`);
            globalClient = client;
        }
    } catch (err) {
        console.error(`${getNowDatetimeString()} getMongoClient`, err);
        globalClient = null;

        throw err;
    }

    return globalClient;
}

//前一次重新建立連線的時間，避免同時觸發太多次
let lastReconnectTime = 0;
//MongoDB重新連線
export async function MongoClientReconnect() {
    const now = Date.now();
    const timeDiff = now - lastReconnectTime;
    if (timeDiff < 1000) {
        return false;
    }
    lastReconnectTime = now;

    if (globalClient) {
        await globalClient.close();
        await globalClient.connect();
    }
    connectToMongo();
    return true;
}

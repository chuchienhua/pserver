import axios from 'axios';
import FormData from 'form-data';
import moment from 'moment';
import qs from 'qs';
export const getNowDatetimeString = () => moment().format('YYYY-MM-DDTHH:mm:ss.SSSZ');

//廠別對應的部門
export const firmToDept = new Map([
    ['7', '17P2'],
    ['A', 'AAP1'],
]);

/**
 * 上傳檔案
 * @param {string} pathName 功能路徑名稱
 * @param {object} file 要上傳的檔案，來源是`req.file`
 * @param {string} fileId 檔案ID，如果是空值為新檔案(upload)，否則為替換檔案(replace)
 * @returns 
 */
export async function uploadFile(pathName, file, fileId = null) {
    let obj = {
        res: null, //上傳成功時則為檔案URL
        error: null, //錯誤訊息
    };

    try {
        //有fileId為替換舊檔案
        const replaceOldFile = !!fileId;

        //產生FormData
        const formData = new FormData();
        formData.append('group_name', 'pbtc-pms');
        formData.append('name', pathName);
        formData.append('token', 'ccp-iot');
        if (replaceOldFile) {
            //replace時的參數名稱跟upload不同
            formData.append('file', file.buffer, {
                filename: file.originalname
            });
            formData.append('file_id', fileId);
        } else {
            formData.append('files', file.buffer, {
                filename: file.originalname
            });
        }

        //呼叫檔案上傳服務API
        const url = 'http://file.ccpgp.com/' + (replaceOldFile ? 'replace' : 'upload');
        let res = await axios.post(url, formData, {
            headers: { 'Content-Type': 'multipart/form-data' },
            proxy: false,
            timeout: 10000,
        }).catch(err => {
            throw err;
        });

        //判斷是否上傳成功
        if (res.data && res.data.error) {
            throw new Error(res.data.error);
        }
        if (!res.data.data || !res.data.data.length) {
            console.error(getNowDatetimeString(), res.data);
            throw new Error('上傳失敗，請重新操作一次');
        }

        //上傳回應結果
        const fileResult = res.data.data[0]; //回傳結果是一個陣列，但FormData那邊只會傳一個檔案
        // console.log(res.data);
        /*
        data: [
            {
            created_time: '2023-01-03T07:36:13.005Z',
            group_name: 'pbtc-iot',
            name: 'packing-detail',
            file_id: '69259b00440c02ae737ce2ec9d2294ef',
            file_name: 'PBT_LABEL (1).png',
            mime_type: 'image/png',
            file_size: 77473,
            client_ip_address: '192.168.102.185',
            readers: [Array],
            reader_orgs: [Array],
            uploader_pps_code: null,
            uploader_name: null,
            _id: '63b3daed37b7056be6b89d87',
            url: 'https://file.ccpgp.com/download/pbtc-iot/packing-detail/69259b00440c02ae737ce2ec9d2294ef'
            }
        ], */

        obj.res = fileResult.url;
    } catch (err) {
        console.error(getNowDatetimeString(), `uploadFile[pathName=${pathName}][fileId=${fileId}]`, err);
        obj.error = err.toString();
    }

    return obj;
}

/**
 * 產生過帳單號
 * @param {string} sheetId 表單編號，例如: PT3
 * @param {Date} invDate 入庫日期
 * @param {boolean} isDebug 是否為測試區
 */
export async function getInvShtNo(sheetId, invDate, isDebug = true) {
    let obj = {
        res: null,
        error: null,
    };

    try {
        //KIND參數規則: SHEET_ID+YYMDD，其中月需要轉換ex. 10->A, 11->B, 12->C
        const kind = sheetId + (invDate.getFullYear() % 100).toString().padStart(2, '0')
            + (invDate.getMonth() + 1).toString(16).toUpperCase()
            + invDate.getDate().toString().padStart(2, '0');
        const postData = [
            {
                DEBUG: !!isDebug, //兩個驚嘆號是轉成boolean
                STABLE: 'PBTC',
                NAME: 'SHTNO',
                KIND: kind,
                LEN: 4
            }
        ];

        const url = 'http://visionservice.ccpgp.com/api/inventory/get_sequence';
        const shtNo = await axios.post(url, postData, {
            proxy: false,
            timeout: 10000,
        }).then(res => {
            if (res.data && Array.isArray(res.data) && res.data.length) {
                return res.data[0];
            }
        }).catch(err => {
            throw err;
        });

        if (shtNo) {
            obj.res = shtNo;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), `getInvShtNo[sheetId=${sheetId}]`, err);
        obj.error = err.toString();
    }

    return obj;
}

/**
 * 產生儲位入庫日期序號 INDATESEQ
 * @param {String} invDate 入庫日期/YYYYMMDD
 */
export async function getInvInDateSeq(invDate) {
    let obj = {
        res: null,
        error: null,
    };

    try {
        //KIND參數規則: 當天日期，格式為YYYYMMDD
        const postData = [
            {
                DEBUG: true,
                STABLE: 'LOCINV_D',
                NAME: 'INDATESEQ',
                KIND: invDate,
                LEN: 5
            }
        ];

        const url = 'http://visionservice.ccpgp.com/api/inventory/get_sequence';
        const inDateSeq = await axios.post(url, postData, {
            proxy: false,
            timeout: 10000,
        }).then(res => {
            if (res.data && Array.isArray(res.data) && res.data.length) {
                return res.data[0];
            }
        }).catch(err => {
            throw err;
        });

        if (inDateSeq) {
            obj.res = inDateSeq;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getInvInDateSeq', err);
        obj.error = err.toString();
    }

    return obj;
}

/**
 * 取得成品標籤棧板編號
 */
export async function getOpno(user) {
    let obj = {
        res: null,
        error: null,
    };

    try {
        const postData = {
            COMPANY: user.COMPANY,
            FIRM: user.FIRM,
            DEPT: user.DEPT,
        };

        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/getOpno';
        const opno = await axios.post(url, qs.stringify(postData), {
            proxy: false,
            timeout: 10000,
        }).then(res => {
            if (res.data && res.data.length) {
                return res.data;
            }
        }).catch(err => {
            throw err;
        });

        if (opno) {
            obj.res = opno;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'getOpno', err);
        obj.error = err.toString();
    }

    return obj;
}

/**
 * 取得ERP出入庫日期，如果時間為00:00~08:00則轉到昨天的帳上
 * @param {Date} time 當下時間
 */
export function getInvtDate(time) {
    let invtDate = time;
    const hoursFormat = 'HH:mm:ss';
    if (moment(time, hoursFormat).isBetween(moment('00:00:00', hoursFormat), moment('08:00:00', hoursFormat))) {
        invtDate = moment(time).subtract(1, 'day').toDate();
    }
    return invtDate;
}
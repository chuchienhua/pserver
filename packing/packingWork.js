import { getNowDatetimeString, firmToDept, uploadFile, getInvShtNo } from '../libs.js';
import * as PackingData from './oraclePacking.js';
import axios from 'axios';
import FormData from 'form-data';
import moment from 'moment';

const axiosConfig = {
    proxy: false,
    timeout: 5000,
};

/**
 * 取得棧板編號
 * @param {*} user 
 * @returns 
 */
export async function getPalletNo(user) {
    const obj = {
        res: null,
        error: null,
    };

    try {
        //產生FormData
        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);

        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/getOpno';
        await axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            if (res.data) {
                obj.res = res.data;
            }
        }).catch(err => {
            throw err;
        });
    } catch (err) {
        console.error(getNowDatetimeString(), 'getPalletNo', err.toString());
        obj.error = err.toString();
    }

    return obj;
}

//列印包裝標籤
export async function printLabel(user, scheduleData, detailData, printerData) {
    const obj = {
        res: null,
        detailData: null,
        error: null,
    };

    try {
        //是否為第一次列印
        let isFirstPrint = (!detailData.PALLET_NO || !detailData.PALLET_NO.match(/^\w/));
        if (isFirstPrint) {
            //產生棧板編號
            const palletNoResult = await getPalletNo(user);
            if (palletNoResult.error) {
                throw new Error(`產生棧板編號失敗! ${palletNoResult.error}`);
            }
            detailData.PALLET_NO = palletNoResult.res;

            //產生列印時間
            detailData.PRINT_LABEL_TIME = new Date();
        }

        obj.detailData = detailData;

        //續包棧板特殊規則
        //印出來的包數、起始序號會接續前一個棧板
        //故 包裝畫面、過帳的數量 會跟標籤上不同
        let seqStart = detailData.DETAIL_SEQ_START;
        if ('續包棧板' === detailData.DETAIL_NOTE) {
            seqStart = detailData.LABEL_SEQ_START ?? detailData.DETAIL_SEQ_START;
        }

        //產生FormData
        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);
        formData.append('PPS_CODE', user.PPS_CODE);
        formData.append('UKEY', scheduleData.PACKING_SEQ);
        formData.append('PRINTER_IP', printerData.PRINTER_IP); //標籤機IP
        formData.append('TAG_KIND', 'ASRS_TAG'); //標籤種類
        formData.append('PRD_PC', scheduleData.PRD_PC);
        formData.append('LOT_NO', scheduleData.LOT_NO);
        formData.append('PACK_WEIGHT', scheduleData.PACKING_WEIGHT_SPEC); //包裝別 25KG
        const packingNumber = Math.max(0, ~~(detailData.DETAIL_SEQ_END - seqStart + 1 - detailData.SEQ_ERROR_COUNT));
        formData.append('PACK_NUM', packingNumber); //一個棧板的包數
        formData.append('G_WEIGHT', Number(scheduleData.PACKING_WEIGHT_SPEC) * packingNumber); //棧板總重(25*40)
        formData.append('PACK_NAME', scheduleData.PACKING_MATERIAL); //包裝性質
        formData.append('PACK_NO', scheduleData.PACKING_MATERIAL_ID); //包裝性質代號 P40
        formData.append('UNIT', 'Kg');
        formData.append('OPNO', detailData.PALLET_NO); //棧板編號
        formData.append('PRINT_DATETIME', moment(detailData.PRINT_LABEL_TIME).format('YYYY/MM/DD HH:mm:ss')); //列印時間
        formData.append('REPRINT', isFirstPrint ? 'false' : 'true'); //是否為重印
        formData.append('PALLET_NAME', scheduleData.PACKING_PALLET_NAME); //棧板別名稱
        formData.append('SEQ_START', seqStart); //包裝起始序號
        formData.append('SEQ_END', detailData.DETAIL_SEQ_END); //包裝結束序號

        //紀錄呼叫API花費的時間
        const timeName = `${getNowDatetimeString()} printAsrsTag ${detailData.PALLET_NO} ${printerData.PRINTER_IP}花費時間`;
        console.time(timeName);
        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/printAsrsTag';
        await axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            console.timeEnd(timeName);
            if ('PRINT_OK' === res.data) {
                obj.res = true;
            } else {
                obj.error = res.data;
            }
            return res.data;
        }).catch(err => {
            console.timeEnd(timeName);
            throw err;
        });
    } catch (err) {
        console.error(getNowDatetimeString(), 'printLabel', err.toString());
        obj.error = err.toString();
    }

    return obj;
}

//結束包裝與過帳
export async function finishPacking(user, scheduleData, newPackingStatus = '已完成') {
    const obj = {
        res: null,
        INV_TIME: null,
        error: null,
    };

    try {
        if (!scheduleData) {
            throw new Error('包裝排程資料為空');
        }
        if (!scheduleData.PACKING_SEQ) {
            throw new Error('包裝排程缺少PACKING_SEQ欄位，請先儲存');
        }

        const packingScheduleResult = await PackingData.getPackingScheduleBySeq(null, user, scheduleData.PACKING_SEQ);
        if (packingScheduleResult.error) {
            throw new Error(packingScheduleResult.error);
        }
        if (!packingScheduleResult.schedule) {
            throw new Error('查無包裝排程');
        }
        const scheduleFromDB = packingScheduleResult.schedule;

        const packingDetailsResult = await PackingData.getPackingDetail(user, scheduleFromDB.PACKING_SEQ);
        if (packingDetailsResult.error) {
            throw new Error(packingDetailsResult.error);
        }
        if (!packingDetailsResult.details || !packingDetailsResult.details.length) {
            throw new Error('查無包裝資料');
        }
        const firstDetailRow = packingDetailsResult.details[0];
        if (PackingData.isPackingStatusFinish(firstDetailRow.PACKING_STATUS)) {
            throw new Error('包裝狀態已結束或取消，不可重複執行結束');
        }

        //計算包裝重量
        let totalPackingNumber = 0; //總包數
        packingDetailsResult.details.forEach(detailData => {
            //跳過已過帳的資料
            if (detailData.INV_TIME) {
                return;
            }
            const packingNumber = Math.max(0, ~~(detailData.DETAIL_SEQ_END - detailData.DETAIL_SEQ_START + 1 - detailData.SEQ_ERROR_COUNT));
            totalPackingNumber += packingNumber;
        });
        const packingWeightSpec = Number(scheduleFromDB.PACKING_WEIGHT_SPEC); //包裝別(KG)
        const totalPackingWeight = totalPackingNumber * packingWeightSpec; //總重量

        if (totalPackingWeight <= 0) {
            console.log(getNowDatetimeString(), `數量為0，不執行包裝過帳, SEQ=${scheduleFromDB.PACKING_SEQ}`);
            return obj;
        }

        //過帳基本資料
        const invDate = new Date(scheduleFromDB.PACKING_DATE);
        const sheetId = 'PT3';
        const dept = firmToDept.get(scheduleFromDB.FIRM) || user.DEPT;
        const invRow = {
            DEBUG: true, //(dept[1] !== '7'), //仁武false正式區；漳州true測試區
            SHEET_ID: sheetId,
            SHTNO: '',
            DEPT: dept, //扣帳部門
            DEPT_IN: dept, //轉入部門
            WAHS: 'PT2', //扣帳倉庫
            WAHS_IN: 'PT2P', //轉入倉庫
            INVT_DATE: moment(invDate).format('YYYYMMDD'),
            PRD_PC: scheduleFromDB.PRD_PC,
            PCK_KIND: 0,
            PCK_NO: '*',
            PRD_PC_IN: scheduleFromDB.PRD_PC,
            PCK_KIND_IN: packingWeightSpec, //包裝別 25KG
            PCK_NO_IN: scheduleFromDB.PACKING_MATERIAL_ID, //包裝性質代號 P40
            QTY: totalPackingWeight, //數量
            PLQTY: 0, //棧板數量
            LOT_NO: scheduleFromDB.LOT_NO, //扣帳批號
            LOTNO_IN: scheduleFromDB.LOT_NO, //轉入批號
            LOC: scheduleFromDB.SILO_NO, //扣帳儲位
            LOC_IN: 'PT2PL', //轉入儲位
            INDATESEQ: '',
            CREATOR: user.PPS_CODE,
        };
        //特殊規則: 換包
        if (('' + scheduleFromDB.SILO_NO).indexOf('換包') > -1) {
            invRow.WAHS = invRow.WAHS_IN;
            invRow.PCK_KIND = invRow.PCK_KIND_IN;
            invRow.PCK_NO = '*';
            invRow.LOC = invRow.LOC_IN;
            if ('紙袋換包' === scheduleFromDB.SILO_NO) {
                invRow.PCK_NO = 'P%';
            } else if ('太空袋換包' === scheduleFromDB.SILO_NO) {
                invRow.PCK_NO = 'T%';
            } else if ('八角箱換包' === scheduleFromDB.SILO_NO) {
                invRow.PCK_NO = 'C%';
            }
        }
        //取得INDATESEQ
        const locInvResult = await PackingData.queryInDateSeq(user, invRow, totalPackingWeight);
        if (locInvResult.error) {
            throw new Error('查詢儲位帳失敗: ' + locInvResult.error);
        }
        //產生過帳資料
        const invData = [];
        let remainQty = totalPackingWeight;
        locInvResult.res.forEach(row => {
            const invQty = Math.min(remainQty, row.QTY, 0); //要扣帳的數量
            if (invQty > 0) {
                remainQty -= invQty;
                invData.push({
                    ...invRow,
                    PCK_KIND: row.PCK_KIND,
                    PCK_NO: row.PCK_NO,
                    QTY: invQty,
                    INDATESEQ: row.INDATESEQ,
                });
            }
        });
        if (remainQty > 0) {
            if (invData.length) {
                //情況: 不足的數量全數給扣帳的最後一筆
                invData[invData.length - 1].QTY += remainQty;
            } else if (locInvResult.res.length) {
                //情況: 儲位帳QTY都<=0，將取其中一筆來扣帳
                invData.push({
                    ...invRow,
                    PCK_KIND: locInvResult.res[0].PCK_KIND,
                    PCK_NO: locInvResult.res[0].PCK_NO,
                    QTY: remainQty,
                    INDATESEQ: locInvResult.res[0].INDATESEQ,
                });
            } else {
                //情況: 儲位帳查無資料，將產生一筆不帶INDATESEQ，後面會替換成 SHTNO + X
                invData.push({
                    ...invRow,
                    PCK_KIND: 0,
                    PCK_NO: '*',
                    QTY: remainQty,
                    INDATESEQ: null,
                });
            }
            remainQty -= remainQty;
        }

        for (let i = 0; i < invData.length; i++) {
            //產生過帳單號
            const shtNoResult = await getInvShtNo(sheetId, invDate, invData[i].DEBUG);
            if (shtNoResult.error) {
                throw new Error('產生過帳單號失敗: ' + shtNoResult.error);
            }
            const shtNo = shtNoResult.res;
            invData[i].SHTNO = shtNo;

            if (!invData[i].INDATESEQ) {
                //查無儲位帳的INDATESEQ = (SHTNO + X) 補到13位
                invData[i].INDATESEQ = shtNo.padEnd(13, 'X');
            }
        }

        console.log(getNowDatetimeString(), `執行包裝過帳, SEQ=${scheduleFromDB.PACKING_SEQ}`, invData);

        //執行過帳
        const invSheetUrl = 'http://visionservice.ccpgp.com/api/inventory/inv_sheet';
        const invResult = await axios.post(invSheetUrl, invData, {
            ...axiosConfig,
        }).then(res => {
            console.log(getNowDatetimeString(), `${scheduleFromDB.PACKING_SEQ}過帳結果`, res.data);
            if (res.data && Array.isArray(res.data) && res.data.length) {
                if (res.data[0][2].includes('失敗')) {
                    throw new Error(`${scheduleFromDB.PACKING_SEQ}過帳失敗: ${JSON.stringify(res.data)}`);
                } else {
                    return true;
                }
            }
        }).catch(err => {
            throw new Error(`${scheduleFromDB.PACKING_SEQ}過帳失敗: ${err.toString()}`);
        });
        if (!invResult) {
            throw new Error(`${scheduleFromDB.PACKING_SEQ}過帳失敗`);
        }

        //寫入LOG到DB
        await PackingData.insertInvApiLog(null, user, 'PACKING', invData);

        //更新包裝排程的狀態
        const updateResult = await PackingData.finishPackingSchedule(user, scheduleFromDB, invDate, invData[0].SHTNO, newPackingStatus);
        if (updateResult.error) {
            throw new Error(updateResult.error);
        }

        //產生FormData
        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);
        formData.append('UKEY', scheduleFromDB.PACKING_SEQ);

        //2023-01-05 改成只呼叫不等待結果
        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/finishPrint';
        axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            // if ('WRITE_OK' === res.data) {
            //     obj.res = true;
            // } else {
            //     obj.error = res.data;
            // }
            return res.data;
        }).catch(err => {
            console.error(getNowDatetimeString(), 'finishPrint', err.toString());
        });
        obj.res = true;
        obj.INV_TIME = invDate;
    } catch (err) {
        console.error(getNowDatetimeString(), 'finishPrint', err.toString());
        obj.error = err.toString();
    }

    return obj;
}

//上傳照片
export async function uploadImage(user, detailData, file) {
    const obj = {
        res: null,
        detailData: null,
        error: null,
    };

    try {
        if ('string' === typeof detailData) {
            try {
                detailData = JSON.parse(detailData);
            } catch (err) {
                throw new Error('包裝資料解析失敗。' + err.toString());
            }
        }
        if (!detailData) {
            throw new Error('包裝資料為空');
        }
        if (!detailData.PACKING_SEQ) {
            throw new Error('包裝資料缺少PACKING_SEQ欄位，請先儲存');
        }
        if (!detailData.DETAIL_ID) {
            throw new Error('包裝資料缺少DETAIL_ID欄位，請先儲存');
        }

        let fileId = null;
        if (('string' === typeof detailData.PHOTO_URL) && detailData.PHOTO_URL.length) {
            const tmp = detailData.PHOTO_URL.split('/');
            const tmpId = tmp.pop();
            if (tmpId && (tmpId.length >= 32)) {
                fileId = tmpId;
            }
        }

        //上傳檔案
        const uploadResult = await uploadFile('packing-detail', file, fileId);

        //判斷是否上傳成功
        if (uploadResult.error) {
            throw new Error('照片上傳失敗，請重新操作一次');
        }

        //檔案URL
        detailData.PHOTO_URL = uploadResult.res;

        const saveResult = await PackingData.savePackingDetail(user, [detailData]);
        if (saveResult.error) {
            obj.error = saveResult.error;
        } else {
            obj.res = true;
            obj.detailData = detailData;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'uploadImage', err);
        obj.error = err.toString();
    }

    return obj;
}

/**
 * 取得殘包標籤編號
 * @param {*} user 
 * @returns 
 */
export async function getRemainderLabelNo(user) {
    const obj = {
        res: null,
        error: null,
    };

    try {
        //產生FormData
        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);

        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/getRemainingPbtOpno';
        await axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            if (res.data) {
                obj.res = res.data;
            }
        }).catch(err => {
            throw err;
        });
    } catch (err) {
        console.error(getNowDatetimeString(), 'getRemainderLabelNo', err.toString());
        obj.error = err.toString();
    }

    return obj;
}

//列印殘包標籤
export async function printRemainderLabel(user, scheduleData, printerData) {
    const obj = {
        res: null,
        schedule: null,
        error: null,
    };

    try {
        const packingScheduleResult = await PackingData.getPackingScheduleBySeq(null, user, scheduleData.PACKING_SEQ);
        if (packingScheduleResult.error) {
            throw new Error(packingScheduleResult.error);
        }
        if (!packingScheduleResult.schedule) {
            throw new Error('查無包裝排程');
        }
        const scheduleFromDB = packingScheduleResult.schedule;

        //是否為第一次列印
        let isFirstPrint = (!scheduleFromDB.REMAINDER_NO);
        if (isFirstPrint) {
            if ('number' !== typeof scheduleData.REMAINDER_WEIGHT) {
                scheduleData.REMAINDER_WEIGHT = Number(scheduleData.REMAINDER_WEIGHT);
                if (isNaN(scheduleData.REMAINDER_WEIGHT)) {
                    throw new Error('殘包重量必須為數值型態');
                } else if (scheduleData.REMAINDER_WEIGHT < 0) {
                    throw new Error('殘包重量必須大於等於0');
                }
            }

            //產生棧板編號
            const palletNoResult = await getRemainderLabelNo(user);
            if (palletNoResult.error) {
                throw new Error(`產生棧板編號失敗! ${palletNoResult.error}`);
            }
            const updateData = {
                REMAINDER_NO: palletNoResult.res,
                REMAINDER_WEIGHT: scheduleData.REMAINDER_WEIGHT,
                REMAINDER_PRINT_LABEL_TIME: new Date(),
            };

            //更新殘包資料到DB
            const packingScheduleUpdateResult = await PackingData.updatePackingSchedule(null, user, scheduleFromDB.PACKING_SEQ, updateData);
            if (packingScheduleUpdateResult.error) {
                throw new Error('包裝排程更新失敗!' + packingScheduleUpdateResult.error);
            }

            Object.assign(scheduleFromDB, updateData);
        }
        //殘包參數以DB為主
        scheduleData.REMAINDER_NO = scheduleFromDB.REMAINDER_NO;
        scheduleData.REMAINDER_WEIGHT = scheduleFromDB.REMAINDER_WEIGHT;
        scheduleData.REMAINDER_PRINT_LABEL_TIME = scheduleFromDB.REMAINDER_PRINT_LABEL_TIME;

        obj.schedule = scheduleData;

        //產生FormData
        const formData = new FormData();
        formData.append('COMPANY', user.COMPANY);
        formData.append('FIRM', user.FIRM);
        formData.append('DEPT', user.DEPT);
        formData.append('PPS_CODE', user.PPS_CODE);
        formData.append('PRINTER_IP', printerData.PRINTER_IP); //標籤機IP
        formData.append('LOT_NO', scheduleFromDB.LOT_NO);
        formData.append('PRD_PC', scheduleFromDB.PRD_PC);
        formData.append('PACK_NO', scheduleFromDB.PACKING_MATERIAL_ID); //包裝性質代號 P40
        formData.append('OPNO', scheduleFromDB.REMAINDER_NO); //殘包編號
        formData.append('WEIGHT', scheduleFromDB.REMAINDER_WEIGHT); //殘包重量
        formData.append('CRT_TIME', moment(scheduleFromDB.REMAINDER_PRINT_LABEL_TIME).format('YYYY/MM/DD HH:mm:ss')); //列印時間
        formData.append('TAG_KIND', 'RM'); //標籤種類
        formData.append('REPRINT', isFirstPrint ? 'false' : 'true'); //是否為重印

        // console.log(formData.getBuffer().toString());

        const url = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/printRemainingPbtTag';
        await axios.post(url, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }).then(res => {
            if ('PRINT_OK' === res.data) {
                obj.res = true;
            } else {
                obj.error = res.data;
            }
            return res.data;
        }).catch(err => {
            throw err;
        });
    } catch (err) {
        console.error(getNowDatetimeString(), 'printRemainderLabel', err.toString());
        obj.error = err.toString();
    }

    return obj;
}

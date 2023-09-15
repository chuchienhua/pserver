import axios from 'axios';
import moment from 'moment';
import qs from 'qs';
import FormData from 'form-data';
import { getNowDatetimeString } from './libs.js';

//共用API回傳值
const obj = {
    res: '',
    error: false,
};

const axiosConfig = {
    proxy: false,
    timeout: 20000,
};

//轉換班別為英文
const workShiftConvert = workShift => {
    switch (workShift) {
        case '早':
            return 'A';
        case '中':
            return 'B';
        case '晚':
            return 'C';
    }
};

//豪哥列印拌粉標籤API
export const printLabelAPI = async (printDate, workShift, lotNo, material, weight, unit, printerIP, tagKind, user) => {

    //台北公司列印標籤切換，為方便測試用
    if ('192.168.102.141' === printerIP) {
        if ('MATERIAL' === tagKind) {
            tagKind = 'MATERIAL_TEST';
        } else if ('MIX' === tagKind) {
            tagKind = 'MIX_TEST';
        }
    }

    const bodyData = qs.stringify({
        PRINT_DATE: moment(printDate).format('YYYY/MM/DD'),
        WORK_SHIFT: workShiftConvert(workShift),
        LOT_NO: lotNo,
        MATERIAL: material,
        WEIGHT: weight,
        UNIT: unit,
        PRINTER_IP: printerIP,
        COMPANY: '' + user.COMPANY,
        FIRM: '' + user.FIRM,
        TAG_KIND: tagKind,
    });

    const PRINT_LABEL_API = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/PrintPbtcMaterialTag';
    const timeName = `${getNowDatetimeString()} printLabelAPI ${printerIP}花費時間`;
    try {
        console.time(timeName);
        const apiResult = await axios.post(PRINT_LABEL_API, bodyData, { ...axiosConfig });
        console.timeEnd(timeName);
        console.log(apiResult.data);
        if ('True' !== apiResult.data) {
            obj.res = apiResult.data;
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printLabelAPI', err.toString());
        console.timeEnd(timeName);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
};

//豪哥列印原料棧板API
export const printPalletAPI = async (printDate, workShift, lotNo, material, printerIP, user) => {

    const bodyData = qs.stringify({
        PRINT_DATE: moment(printDate).format('YYYY/MM/DD'),
        WORK_SHIFT: workShiftConvert(workShift),
        LOT_NO: lotNo,
        MATERIAL: JSON.stringify(material),
        PRINTER_IP: printerIP,
        COMPANY: '' + user.COMPANY,
        FIRM: '' + user.FIRM,
        TAG_KIND: 'MATERIAL_DETAIL',
    });

    const PRINT_PALLET_API = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/PrintPbtcDetailMaterialTag';
    const timeName = `${getNowDatetimeString()} printPalletAPI ${printerIP}花費時間`;
    try {
        console.time(timeName);
        const apiResult = await axios.post(PRINT_PALLET_API, bodyData, { ...axiosConfig });
        console.timeEnd(timeName);
        console.log(apiResult.data);
        if ('True' !== apiResult.data) {
            obj.res = apiResult.data;
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printPalletAPI', err.toString());
        console.timeEnd(timeName);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
};

//列印餘料、拌粉機、入料機標籤
export const printMachineAPI = async (printerIP, printData, user) => {

    const PRINT_MACHINE_API = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/PrintIndicationTag';
    const timeName = `${getNowDatetimeString()} printMachineAPI ${printerIP}花費時間`;
    try {
        console.time(timeName);
        const apiResult = await axios.post(PRINT_MACHINE_API, qs.stringify({
            COMPANY: '' + user.COMPANY,
            FIRM: '' + user.FIRM,
            PRINTER_IP: printerIP,
            DATA: printData,
            TAG_KIND: 'SHOW_DATA',
        }), { ...axiosConfig });
        console.timeEnd(timeName);
        if ('True' !== apiResult.data) {
            obj.res = apiResult.data;
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printMachineAPI', err.toString());
        console.timeEnd(timeName);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
};

//列印料頭回收太空帶標籤
export const printScrapAPI = async (printerIP, bagSeries, batchNo, user) => {

    const PRINT_SCRAP_API = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/PrintScrapTag';
    const timeName = `${getNowDatetimeString()} printScrapAPI ${printerIP}花費時間`;
    try {
        console.time(timeName);
        const apiResult = await axios.post(PRINT_SCRAP_API, qs.stringify({
            COMPANY: '' + user.COMPANY,
            FIRM: '' + user.FIRM,
            PRINTER_IP: printerIP,
            SCRAP: bagSeries,
            BATCH_NO: batchNo,
            CRT_TIME: moment(new Date()).format('YYYYMMDDhhmm'),
            TAG_KIND: 'SCRAP',
        }), { ...axiosConfig });
        console.timeEnd(timeName);
        if ('True' !== apiResult.data) {
            obj.res = apiResult.data;
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printScrapAPI', err.toString());
        console.timeEnd(timeName);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
};

//列印成品標籤
export const printProductAPI = async (printerIP, opno, batchNo, productNo, packNum, packWeight, totalWeight, packNo, packName, user) => {

    //產生FormData
    const formData = new FormData();
    formData.append('COMPANY', user.COMPANY);
    formData.append('FIRM', user.FIRM);
    formData.append('DEPT', user.DEPT);
    formData.append('PPS_CODE', user.PPS_CODE);
    formData.append('UKEY', batchNo);
    formData.append('PRINTER_IP', printerIP); //標籤機IP
    formData.append('TAG_KIND', 'OFF'); //標籤種類，改為OFF
    formData.append('PRD_PC', productNo);
    formData.append('LOT_NO', batchNo);
    formData.append('PACK_WEIGHT', packWeight);
    formData.append('PACK_NUM', packNum); //一個棧板的包數
    formData.append('G_WEIGHT', totalWeight); //棧板總重(25*40)
    formData.append('PACK_NAME', packName); //包裝性質
    formData.append('PACK_NO', packNo); //包裝性質代號 P40
    formData.append('UNIT', 'Kg');
    formData.append('OPNO', opno); //棧板編號
    formData.append('PRINT_DATETIME', moment(new Date()).format('YYYY/MM/DD HH:mm:ss')); //列印時間
    formData.append('REPRINT', 'false'); //是否為重印

    const PRINT_ASRS_API = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/printAsrsTag';
    const timeName = `${getNowDatetimeString()} printProductAPI ${printerIP}花費時間`;
    try {
        console.time(timeName);
        const apiResult = await axios.post(PRINT_ASRS_API, formData, {
            ...axiosConfig,
            headers: {
                'Content-Type': 'multipart/form-data',
            }
        });
        console.timeEnd(timeName);
        if ('PRINT_OK' !== apiResult.data) {
            obj.res = apiResult.data;
            obj.error = true;
        }
    } catch (err) {
        console.error(getNowDatetimeString(), 'printProductAPI', err.toString());
        console.timeEnd(timeName);
        obj.res = err.toString();
        obj.error = true;
    }

    return obj;
};
import axios from 'axios';
import qs from 'qs';

//豪哥檔案維護頁面
export const getPage = (pageName, user) => {

    let pageURL;
    switch (pageName) {
        case 'printer':
            pageURL = 'https://tpiot.ccpgp.com:81/PrinterInfoConfig/PrinterInfoConfig';
            break;
        default:
            break;
    }

    return axios.post(pageURL, qs.stringify({
        COMPANY: user.COMPANY,
        FIRM: user.FIRM,
        DEPT: user.DEPT,
        PPS_CODE: user.PPS_CODE,
        USER_NAME: user.NAME,
    }), { proxy: false });
};
import fs from 'fs';

const config = {
    secret: fs.readFileSync('/opt/CCPSSO.Core/SSO_TOKEN.key').toString(),
    ComeFrom: 1010000000000000,
    NAME: 'kh_pbtc',
    HTTP_PORT: 10010,
    ORACLE_TNS: 'MIS_TEST.CCP.COM.TW', //測試:'MIS_TEST.CCP.COM.TW' 正式:203.69.132.121:1521/erp
    // ORACLE_TNS: 'MIS_TEST.CCP.COM.TW', //測試:'MIS_TEST.CCP.COM.TW' 正式:203.69.132.121:1521/erp
    ORACLE_USERNAME: 'AC',
    ORACLE_PASSWORD: 't5001855',
    ORACLE_CONFIG: {},
};

//Oracle DB連線設定
config.ORACLE_CONFIG = {
    user: config.ORACLE_USERNAME,
    password: config.ORACLE_PASSWORD,
    connectString: config.ORACLE_TNS,
};

export default config;
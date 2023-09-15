process.env.TZ = "Asia/Taipei";

import config from "./config.js";
import * as libs from "./libs.js";
import * as mixDB from "./mixing/oracleMix.js";
import * as authDB from "./oracleAuth.js";
import * as extrusionDB from "./oracleExtrusion.js";
import * as storageDB from "./extrusion/oracleStorage.js";
import * as maintainPage from "./maintainPage.js";
import * as PrinterAPI from "./printLabel.js";
import * as scheduleMonitor from "./scheduleMonitor.js";
import * as recipeDB from "./recipe/oracleRecipe.js";
import * as siloDB from "./siloStatus/oracleSiloStatus.js";
import recipeRouter from "./router/recipeRouter.js";
import packingRouter from "./router/packingRouter.js";
import * as PackingData from "./packing/oraclePacking.js";
import mixingRouter from "./router/mixingRouter.js";
import extrusionRouter from "./router/extrusionRouter.js";
import traceRouter from "./router/traceRouter.js";
import managementRouter from "./router/managementRouter.js";
import * as remainingData from "./packing/oraclePackingRemain.js";
import remainBagRouter from "./router/remainBagRouter.js";
import extrusionSummaryRouter from "./router/extrusionSummaryRouter.js";
import * as summaryReport from "./extrusion/oracleSummaryReport.js";
import siloRouter from "./router/siloStatusRouter.js";
import stockSettingRouter from "./router/stockSettingRouter.js";
import * as Mailer from "./mailer.js";
import express from "express";
const app = express();

import jwt from "jsonwebtoken";
import axios from "axios";
import compression from "compression";
import bodyParser from "body-parser";
import cron from "cron";
import os from "os";
import moment from "moment";

// parse application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));
// parse application/json
app.use(express.json({ limit: "20mb" }));
// parse form-data
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

//整合jwt
app.set("superSecret", config.secret); //jwt密鑰
const apiRoutes = express.Router();
const apiAllowList = new Set(["/login", "/loginJWT"]); //忽略token檢查的API允許名單

//廠別別對應的員工編號權限
let userFirmMap = new Map([
  ["7", new Set()], //高雄廠
  ["A", new Set()], //漳州廠
]);
//刷新廠別與員工編號的Map
async function resetUserFirmMap() {
  let arr = await authDB.getFirmUser();
  if (arr.length) {
    const tempMap = arr.reduce((map, row) => {
      if (!map.has(row.FIRM)) {
        map.set(row.FIRM, new Set());
      }
      map.get(row.FIRM).add(row.PPS_CODE);
      return map;
    }, new Map());
    userFirmMap = tempMap;
  }
}

process.on("unhandledRejection", (reason) => {
  console.error(`${libs.getNowDatetimeString()} Unhandled Rejection:`, reason);
});
process.on("uncaughtException", (err) => {
  console.error(`${libs.getNowDatetimeString()} Uncaught Exception:`, err);
});

apiRoutes.use((req, res, next) => {
  let token = null;
  if (
    req.headers.authorization &&
    "Bearer" === req.headers.authorization.split(" ")[0]
  ) {
    token = req.headers.authorization.split(" ")[1];
  } else {
    token = req.body.token || req.query.token || req.headers["x-access-token"];
  }
  let statusCode = 200;
  let errMsg = "";

  req.user = {};
  if (token) {
    let expireSetting =
      "/loginJWT" === req.path ? {} : { ignoreExpiration: true }; //僅在Refresh時重新驗證Token期限
    jwt.verify(token, app.get("superSecret"), expireSetting, (err, decoded) => {
      if (err || !req.headers.firm) {
        statusCode = 403;
        errMsg = "Failed to authenticate token.";
      } else {
        //如果廠別存在於Map中，則允許繼續
        if (userFirmMap.get(req.headers.firm).has(decoded.PPS_CODE)) {
          if ("7" === req.headers.firm) {
            decoded.COMPANY = "1";
            decoded.FIRM = "7";
            decoded.DEPT = "17P2";
          } else if ("A" === req.headers.firm) {
            decoded.COMPANY = "A";
            decoded.FIRM = "A";
            decoded.DEPT = "AAP1";
          } else {
            statusCode = 403;
            errMsg = "Failed to read firm header.";
          }
        }

        req.user = decoded;
        /*
                    {
                    COMPANY: '1',
                    FIRM: '1',
                    FIRM_NAME: '長春樹脂台北公司',
                    DEPT_NO: '11MIMI',
                    DEPT_NO_TW: null,
                    PPS_CODE: '23296',
                    NAME: '廖建銘',
                    DUTY_CODE: 'CNA',
                    EMAIL: 'chien_ming_liao@ccpgp.com',
                    ORG_NO: 'CCPG-WGROUP-11-MI-IOT',
                    ORG_NAME: '資訊中心IoT',
                    REAL_USER: null,
                    SESSION_ID: 'ed0e8a6f6099d23a53f77135e5a416',
                    VISION_TYPE: 'ADMIN',
                    ISENDER_NUM: '88212',
                    ISENDER_UUID: '8156daaf5035f0ed_8156daaf5035f0ed',
                    ss_exp: '2022/12/15 03:07:58',
                    exp: 1670988277
                    }
                */
      }
    });
  } else {
    statusCode = 403;
    errMsg = "No token provided.";
  }

  //為了開發方便從localhost進來的連線可以忽略token
  if (String(req.headers.host).startsWith("localhost")) {
    return next();
  } else if ("OPTIONS" === req.method) {
    //方便local測試PDA用
    return next();
  } else if (apiAllowList.has(req.path) > -1) {
    return next();
  } else if (200 !== statusCode) {
    return res.status(statusCode).json({ res: errMsg, error: true });
  }
  return next();
});

//CORS
apiRoutes.use((req, res, next) => {
  if ("OPTIONS" !== req.method) {
    console.log(`${libs.getNowDatetimeString()}; Method:${req.method}; Path:${
      req.path
    }; IP:${req.headers["x-forwarded-for"] || req.socket.remoteAddress};
            ${
              req.user &&
              ` USER:${req.user.PPS_CODE}, ${req.user.NAME}; COMPANY:${req.user.COMPANY}; FIRM:${req.user.FIRM}; `
            } `);
  }

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "X-Requested-With, Content-Type, Authorization, Firm"
  );
  res.setHeader("Access-Control-Allow-Credentials", true);

  next();
});

//gzip壓縮
apiRoutes.use(compression());

//每日凌晨00:00跑一遍自動押出領繳程式
if ("TPIOT" === os.hostname()) {
  const autoUser = {
    COMPANY: "1",
    FIRM: "7",
    DEPT: "17P2",
    PPS_CODE: os.hostname(),
    NAME: "系統排程",
  };

  //自動領繳，先測試每10分鐘領繳一次
  const erpInvtJob = new cron.CronJob("0,10,20,30,40,50 * * * *", async () => {
    await storageDB.runPickAndPay(10, autoUser);
  });
  erpInvtJob.start();

  //自動寄信入料品質
  const feedQualityJob = new cron.CronJob("0 8 * * *", async () => {
    const date = moment(new Date()).format("YYYYMMDD");
    let result = await storageDB.getExtrusionQuality(date, "date", autoUser);
    if (!result.error) {
      await Mailer.autoExtrusionQuality(date, result.res, autoUser);
    }
  });
  feedQualityJob.start();

  //排程結束監控
  const scheduleMonitorJob = new cron.CronJob("0 * * * *", async () => {
    console.log("Schedule Monitor Running...");
    scheduleMonitor.scheduleMonitor(autoUser);
  });
  scheduleMonitorJob.start();

  //自動更新SILO儲位
  const siloMonitor = new cron.CronJob("*/10 * * * *", async () => {
    await siloDB.removeEmptySilo(autoUser);
  });
  siloMonitor.start();

  //結束前一天的包裝排程，並自動產生未完成的排程到當天
  const packingScheduleJob = new cron.CronJob("0 8,9 * * *", async () => {
    const today = moment().subtract(1, "days").format("YYYY-MM-DD");
    PackingData.finishPackingScheduleByDay(
      {
        COMPANY: "1",
        FIRM: "7",
        DEPT: "17P2",
        PPS_CODE: "SYSTEM",
        NAME: "系統排程",
      },
      today
    );
  });
  packingScheduleJob.start();

  //停機分析與個人績效週報，每週五早上8:00，
  const weeklyJob = new cron.CronJob("0 8 * * 5", async () => {
    console.log("Weekly Job Running...");
    Mailer.weeklyMonthlyReport("weekly", autoUser);
  });
  weeklyJob.start();

  //停機分析與個人績效月報，每月初一早上8:00，
  const MonthlyJob = new cron.CronJob("0 8 1 * *", async () => {
    console.log("MonthlyJob Job Running...");
    Mailer.weeklyMonthlyReport("monthly", autoUser);
  });
  MonthlyJob.start();

  //殘包存放天數提醒，每周一早上08:00
  const remainingBagStockJob = new cron.CronJob("0 8 * * 1", async () => {
    console.log("RemainingBag Stock Job Running...");
    const endDate = moment().subtract(90, "days").format("YYYYMMDD");
    remainingData.alertStock(autoUser, endDate);
  });
  remainingBagStockJob.start();

  //生產日報表，每天早上08:30
  const summaryDayReport = new cron.CronJob("30 8 * * *", async () => {
    console.log("SummaryDayReport Job Running...");
    const date = moment().format("YYYYMMDD");
    summaryReport.mailDayReport(autoUser, date);
  });
  summaryDayReport.start();

  //生產月報表，每月初一早上08:30
  const summaryMonthReport = new cron.CronJob("30 8 1 * *", async () => {
    console.log("SummaryMonthReport Job Running...");
    const date = moment().format("YYYYMMDD");
    summaryReport.mailMonthReport(autoUser, date);
  });
  summaryMonthReport.start();
}

//登入串接Vision API
apiRoutes.post("/login", function (req, res) {
  const id = req.body.id;
  const pw = req.body.pw;
  const firm = req.body.firm;

  const apiUrl = "https://vision.ccpgp.com/api/common/login";
  axios
    .post(apiUrl, { id: id, pw: pw }, { proxy: false, timeout: 10000 })
    .then((val) => {
      if (val.data.token && firm) {
        authDB.getAllUserAuth(val.data.user.PPS_CODE, firm).then((auth) => {
          if (0 !== auth.length) {
            val.data.authRoutes = auth;
            val.data.firm = firm;
            // console.log("auth", auth);
            // console.log("firm", firm);
          } else {
            val.data.error = "並無此廠的權限";
            val.data.token = null;
          }
          res.send(val.data);
        });
      } else {
        res.send(val.data);
      }
    })
    .catch((err) => {
      console.error(libs.getNowDatetimeString(), "Login Error", err.toString());
      res.send({
        user: null,
        token: null,
        error: err.toString(),
      });
    });
});

apiRoutes.post("/loginJWT", function (req, res) {
  const token = req.body.token;
  const firm = req.body.firm;

  const apiUrl = "https://vision.ccpgp.com/api/common/refresh";
  axios
    .post(apiUrl, { token: token }, { proxy: false })
    .then((val) => {
      if (val.data.token && firm) {
        authDB.getAllUserAuth(val.data.user.PPS_CODE, firm).then((auth) => {
          if (0 !== auth.length) {
            val.data.authRoutes = auth;
            val.data.firm = firm;
          } else {
            val.data.error = "並無此廠的權限";
            val.data.token = null;
          }
          res.send(val.data);
        });
      } else {
        res.send(val.data);
      }
    })
    .catch((err) => {
      console.error(
        libs.getNowDatetimeString(),
        "Refresh login Error",
        err.toString()
      );
      res.send({
        user: null,
        token: null,
        error: err.toString(),
      });
    });
});

//權限管理部分
//取得所有Routes
apiRoutes.get("/routes", function (req, res) {
  authDB.getAllRoutes(req.user).then((val) => res.send(val));
});

//新增使用者權限
apiRoutes.post("/addRouteUser", function (req, res) {
  const ppsCode = req.body.ppsCode;
  const route = req.body.route;
  const isAdmin = req.body.isAdmin;

  authDB.addRouteUser(ppsCode, route, isAdmin, req.user).then((val) => {
    resetUserFirmMap();
    res.send(val);
  });
});

//移除使用者權限
apiRoutes.post("/removeRouteUser", function (req, res) {
  const ppsCode = req.body.ppsCode;
  const route = req.body.route;
  const isAdmin = req.body.isAdmin;

  authDB.removeRouteUser(ppsCode, route, isAdmin, req.user).then((val) => {
    resetUserFirmMap();
    res.send(val);
  });
});

//使用員工編號查詢名字、所屬公司廠別
apiRoutes.get("/getUserData/:ppsCode", function (req, res) {
  const ppsCode = req.params.ppsCode;

  authDB.getUserData(ppsCode).then((val) => res.send(val));
});

//取得所有列印標籤機台
apiRoutes.get("/printer", function (req, res) {
  mixDB.getAllPrinter(req.user).then((val) => res.send(val));
});

//拌粉領料/押出入料前先檢查原料棧板品檢結果
apiRoutes.get(
  "/materialBatchDetail/:material/:lotNo/:batchNo",
  function (req, res) {
    const material = req.params.material;
    const lotNo = req.params.lotNo;
    const batchNo = req.params.batchNo;

    mixDB
      .materialBatchDetail(material, lotNo, batchNo, req.user)
      .then((val) => res.send(val));
  }
);

/* 檔案維護 */
//維護檔案頁面
apiRoutes.get("/maintainPage/:pageName", function (req, res) {
  const pageName = req.params.pageName;

  maintainPage.getPage(pageName, req.user).then((val) => {
    res.setHeader("Content-type", "text/html");
    res.send(val.data);
  });
});

//列印餘料/拌粉機/入料機標籤
apiRoutes.post("/printMachine", function (req, res) {
  const printerIP = req.body.printerIP;
  const printData = req.body.printData;

  PrinterAPI.printMachineAPI(printerIP, printData, req.user).then((val) =>
    res.send(val.res)
  );
});

//拌粉機&入料機檔案維護
apiRoutes.post("/fileMaintain/:table/:operation", function (req, res) {
  const table = req.params.table; // mixer || feeder
  const operation = req.params.operation;
  if ("mixer" === table) {
    const mixer = req.body.mixer;
    if ("create" === operation) {
      mixDB.createMixer(mixer, req.user).then((val) => res.send(val));
    } else if ("delete" === operation) {
      mixDB.deleteMixer(mixer, req.user).then((val) => res.send(val));
    }
  } else if ("feeder" === table) {
    if ("update" === operation) {
      const feeder = req.body.feederArray;
      extrusionDB.updateFeeder(feeder, req.user).then((val) => res.send(val));
    } else {
      const line = req.body.line;
      const feeder = req.body.feeder;
      const tolerance = req.body.toleranceRatio;
      if ("create" === operation) {
        extrusionDB
          .createFeeder(line, feeder, tolerance, req.user)
          .then((val) => res.send(val));
      } else if ("delete" === operation) {
        extrusionDB
          .deleteFeeder(line, feeder, req.user)
          .then((val) => res.send(val));
      }
    }
  } else if ("bagWeight" === table) {
    const productNo = req.body.productNo;
    const line = req.body.line;
    const ver = req.body.ver;
    const bagWeight = req.body.bagWeight;
    recipeDB
      .editBagWeight(productNo, line, ver, bagWeight, req.user)
      .then((val) => res.send(val));
  }
});

//取得入料機
apiRoutes.get("/feeder", function (req, res) {
  extrusionDB.getFeeder(req.user).then((val) => res.send(val));
});

//入料機檔案維護
apiRoutes.get("/feeder/fileMaintain", function (req, res) {
  extrusionDB.getFeederFileMaintain(req.user).then((val) => res.send(val));
});

/* 生產排程 */
//取得指定排程的資料
apiRoutes.get("/schedules/:line?/:sequence?", function (req, res) {
  const line = req.params.line;
  const sequence = req.params.sequence;

  mixDB.getSchedule(line, sequence, req.user).then((val) => res.send(val));
});

/* 配方管理 */
apiRoutes.use("/recipe", recipeRouter);

/* 拌粉 */
apiRoutes.use("/mixing", mixingRouter);

/* 押出作業 TODO:太多了，待整理進去 */
apiRoutes.use("/extrusion", extrusionRouter);

/* 生產追溯 */
apiRoutes.use("/trace", traceRouter);

/* 包裝作業 */
apiRoutes.use("/packing", packingRouter);

/* 殘包作業 */
apiRoutes.use("/remainingBag", remainBagRouter);

/*生產報表 */
apiRoutes.use("/summaryReport", extrusionSummaryRouter);

/* 管理報表 */
apiRoutes.use("/managementReport", managementRouter);

/* SILO看板 */
apiRoutes.use("/siloStatus", siloRouter);

/* 原料/產品安全庫存與上限維護 */
apiRoutes.use("/stockSetting", stockSettingRouter);

//所有api先檢查token
const routerPath = "/api/kh_pbtc_server";
console.log(`${libs.getNowDatetimeString()} routerPath: ${routerPath}`);
app.use(routerPath, apiRoutes);

app.listen(config.HTTP_PORT, () => {
  resetUserFirmMap();
  console.log(
    `KH PBTC Server is listening on port ${config.HTTP_PORT}! ComeFrom=${config.ComeFrom}`
  );
});

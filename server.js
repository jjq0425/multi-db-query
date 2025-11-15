const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const winston = require('winston');
const fs = require('fs').promises;
const path = require('path');
const vm = require('vm');

// 配置日志
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});

// 开发环境下同时输出到控制台
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.simple()
  }));
}

class DatabaseQueryBot {
  constructor() {
    this.app = express();
    this.server = http.createServer(this.app);
    this.wss = new WebSocket.Server({ server: this.server });
    this.connections = new Map();
    this.activeQueries = new Map();
    this.config = null;

    // 初始化并保存初始化完成的 Promise，便于 start 等待配置和调度器就绪
    this._initialized = this.initialize();
  }

  async initialize() {
    await this.loadConfig();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupWebSocket();
    this.setupStaticFiles();
  }

  async loadConfig() {
    try {
      const configPath = path.join(__dirname, 'config.json');
      const configData = await fs.readFile(configPath, 'utf8');
      const parsed = JSON.parse(configData);
      // 合并默认配置，保证新增的配置项有默认值
      this.config = { ...this.getDefaultConfig(), ...parsed };
      logger.info('配置文件加载成功');
    } catch (error) {
      logger.error('配置文件加载失败，使用默认配置', error);
      this.config = this.getDefaultConfig();
      await this.saveConfig();
    }
    // 启动调度器（或在 initialize 后启动）
    this._initScheduler();
  }

  getDefaultConfig() {
    return {
      // 本服务监听端口
      serverPort: 3000,
      // 调度器检查间隔（秒）
      schedulerIntervalSeconds: 30,
      databaseConfigs: [

      ],
      sqlQuery: `SELECT 
    section,
    COUNT(*) as count
FROM record
GROUP BY section
ORDER BY count DESC;`,
      feishuWebhook: '',
      queryTimeout: 600000, // 10分钟
      maxConnections: 5
      ,
      // 可配置的 SQL 模板
      sqlTemplates: [
        {
          id: 'tpl_1',
          name: '查询提交量',
          sql: `SELECT 
    section,
    COUNT(*) as count
FROM record
GROUP BY section
ORDER BY count DESC;`
        }
      ]
      ,
      // 定时任务
      scheduledTasks: []
    };
  }

  async saveConfig() {
    try {
      const configPath = path.join(__dirname, 'config.json');
      await fs.writeFile(configPath, JSON.stringify(this.config, null, 2), 'utf8');
      logger.info('配置文件保存成功');
    } catch (error) {
      logger.error('配置文件保存失败', error);
    }
  }

  setupMiddleware() {
    this.app.use(cors());
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
  }

  // 调度器初始化
  _initScheduler() {
    // 从配置读取调度间隔（秒），默认 30 秒
    const intervalSeconds = (this.config && Number(this.config.schedulerIntervalSeconds)) || 30;
    if (this._schedulerTimer) clearInterval(this._schedulerTimer);
    this._schedulerTimer = setInterval(() => {
      try {
        this.checkScheduledTasks();
      } catch (e) {
        logger.error('调度器检查异常', e);
      }
    }, intervalSeconds * 1000);
    logger.info(`任务调度器已启动，每${intervalSeconds}秒检查一次`);
  }

  // 检查并触发定时任务
  async checkScheduledTasks() {
    console.log('高雅检查定时任务中');
    if (!this.config || !Array.isArray(this.config.scheduledTasks)) return;

    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const current = `${hh}:${mm}`; // 格式 HH:MM

    for (const task of this.config.scheduledTasks) {
      try {
        if (task.disabled) continue;
        // task.scheduleTimes: array of "HH:MM" strings
        const times = Array.isArray(task.scheduleTimes) ? task.scheduleTimes : (task.scheduleTime ? [task.scheduleTime] : []);

        if (!times.includes(current)) continue;

        // 防止同一分钟重复触发：记录 lastRunMinute
        if (task._lastRunMinute === current) continue;
        task._lastRunMinute = current;

        logger.info(`触发定时任务: ${task.name} (${task.id || 'no-id'}) at ${current}`);
        // 执行任务（不阻塞其他任务）
        this.executeScheduledTask(task).catch(err => logger.error('执行定时任务失败', err));
      } catch (err) {
        logger.error('单个任务检查失败', err);
      }
    }
  }

  // 执行单个定时任务
  async executeScheduledTask(task) {
    // task: { id, name, scheduleTimes, sql, dbIds, feishuWebhook }
    const taskId = `task_${task.id || uuidv4()}`;
    const dbIds = task.dbIds || [];
    const databases = this.config.databaseConfigs.filter(db => dbIds.includes(db.id) && db.enabled);
    if (databases.length === 0) {
      logger.warn(`任务 ${task.name} 没有可用数据库，跳过`);
      return null;
    }

    const queryRecord = {
      id: taskId,
      status: 'running',
      startTime: new Date().toISOString(),
      databases: databases.map(d => d.id),
      results: {}
    };
    databases.forEach(db => {
      queryRecord.results[db.id] = {
        status: 'running',
        database: db.name,
        dbInfo: db,
        databaseId: db.id,
        startTime: new Date().toISOString()
      };
    });

    // 立即登记为活动查询
    this.activeQueries.set(queryRecord.id, queryRecord);

    // 异步执行任务，不阻塞调用者；在内部完成后更新状态并发送通知
    (async () => {
      try {
        const promises = databases.map(async (db) => {
          const result = await this.executeSingleQueryWithSQL(db, task.sql || this.config.sqlQuery);
          queryRecord.results[db.id] = result;
          return result;
        });

        await Promise.all(promises);

        queryRecord.status = 'completed';
        queryRecord.endTime = new Date().toISOString();

        // 发送任务级飞书通知（优先使用任务指定 webhook，否则使用全局 webhook）
        const webhook = task.feishuWebhook && task.feishuWebhook.length > 0 ? task.feishuWebhook : (this.config.feishuWebhook || '');
        if (webhook) {
          await this.sendFeishuNotificationToWebhookSchedual(queryRecord, webhook, task.name, task.notificationScript || null);
        }

      } catch (error) {
        queryRecord.status = 'failed';
        queryRecord.error = error.message;
        queryRecord.endTime = new Date().toISOString();
        logger.error(`定时任务执行失败: ${task.name}`, error);
      } finally {
        // 在活动查询中保留一段时间以便审计，然后清理
        setTimeout(() => this.activeQueries.delete(queryRecord.id), 5 * 60 * 1000);
      }
    })();

    // 返回立即可用的 query id
    return queryRecord.id;
  }

  // 使用指定 SQL 执行单数据库查询（不会修改全局 config.sqlQuery）
  async executeSingleQueryWithSQL(dbConfig, sql) {
    const startTime = new Date();
    let connection;
    try {
      logger.info(`开始执行定时任务查询: ${dbConfig.name}`);
      connection = await mysql.createConnection({
        host: dbConfig.host,
        port: dbConfig.port,
        user: dbConfig.user,
        password: dbConfig.password,
        database: dbConfig.database,
        connectTimeout: 30000,
        timeout: this.config.queryTimeout
      });

      const [rows] = await connection.execute(sql);
      const endTime = new Date();
      return {
        status: 'success',
        database: dbConfig.name,
        databaseId: dbConfig.id,
        dbInfo: dbConfig,
        startTime: startTime.toISOString(),
        endTime: endTime.toISOString(),
        executionTime: endTime - startTime,
        results: rows,
        rowCount: rows.length
      };
    } catch (error) {
      const endTime = new Date();
      logger.error(`定时任务查询失败: ${dbConfig.name}`, error);
      return {
        status: 'failed',
        database: dbConfig.name,
        databaseId: dbConfig.id,
        dbInfo: dbConfig,
        startTime: startTime.toISOString(),
        endTime: endTime.toISOString(),
        executionTime: endTime - startTime,
        error: error.message
      };
    } finally {
      if (connection) {
        try { await connection.end(); } catch (e) { logger.error('关闭连接失败', e); }
      }
    }
  }

  // 针对定时任务发送飞书通知（使用指定 webhook）
  async sendFeishuNotificationToWebhookSchedual(queryRecord, webhookUrl, taskName, customJs = null) {
    try {
      console.log('发送飞书通知 (schedule)', queryRecord);
      // 将自定义 JS 代码传入 sendFeishuNotification
      this.sendFeishuNotification(queryRecord, webhookUrl, true, customJs);
    } catch (error) {
      logger.error('任务通知发送异常', error);
    }
  }

  setupRoutes() {
    // 获取配置
    this.app.get('/api/config', (req, res) => {
      res.json(this.config);
    });

    // 更新配置
    this.app.post('/api/config', async (req, res) => {
      try {
        // 合并默认配置、当前配置和前端提交，避免某些字段被意外删除或回退到错误值
        this.config = { ...this.getDefaultConfig(), ...this.config, ...req.body };
        await this.saveConfig();
        res.json({ success: true, message: '配置更新成功' });
      } catch (error) {
        res.status(500).json({ success: false, message: '配置更新失败', error: error.message });
      }
    });

    // 测试数据库连接
    this.app.post('/api/test-connection', async (req, res) => {
      try {
        const { host, port, user, password, database } = req.body;
        const connection = await mysql.createConnection({
          host,
          port,
          user,
          password,
          database,
          connectTimeout: 10000
        });
        await connection.end();
        res.json({ success: true, message: '连接成功' });
      } catch (error) {
        res.json({ success: false, message: '连接失败', error: error.message });
      }
    });

    // 执行查询
    this.app.post('/api/execute-query', async (req, res) => {
      try {
        const queryId = uuidv4();
        const { dbIds = [] } = req.body;

        if (dbIds.length === 0) {
          return res.status(400).json({ success: false, message: '请选择至少一个数据库' });
        }

        const databases = this.config.databaseConfigs.filter(db =>
          dbIds.includes(db.id) && db.enabled
        );

        if (databases.length === 0) {
          return res.status(400).json({ success: false, message: '没有可用的数据库' });
        }

        // 创建查询记录，并为每个数据库预先填充结果对象（初始状态为 running）
        const queryRecord = {
          id: queryId,
          status: 'running',
          startTime: new Date().toISOString(),
          databases: databases.map(db => db.id),
          results: {}
        };

        // 为每个数据库创建初始结果（即使查询尚未完成），确保前端可以正确计算总数和状态
        databases.forEach(db => {
          queryRecord.results[db.id] = {
            status: 'running',
            database: db.name,
            dbInfo: db,
            databaseId: db.id,
            startTime: new Date().toISOString()
          };
        });

        this.activeQueries.set(queryId, queryRecord);



        // 异步执行查询
        this.executeQueriesAsync(queryId, databases);

        res.json({ success: true, queryId });
      } catch (error) {
        res.status(500).json({ success: false, message: '查询启动失败', error: error.message });
      }
    });

    // 获取查询状态
    this.app.get('/api/query-status/:queryId', (req, res) => {
      const { queryId } = req.params;
      const query = this.activeQueries.get(queryId);

      if (!query) {
        return res.status(404).json({ success: false, message: '查询不存在' });
      }

      res.json(query);
    });

    // 清空日志文件
    this.app.post('/api/clear-logs', async (req, res) => {
      try {
        const errorLog = path.join(__dirname, 'error.log');
        const combinedLog = path.join(__dirname, 'combined.log');

        // 覆盖为空字符串，保留文件和权限
        await fs.writeFile(errorLog, '', 'utf8');
        await fs.writeFile(combinedLog, '', 'utf8');

        logger.info('日志文件已清空');
        res.json({ success: true, message: '日志已清空' });
      } catch (error) {
        logger.error('清空日志失败', error);
        res.status(500).json({ success: false, message: '清空日志失败', error: error.message });
      }
    });

    // 立即触发指定索引的定时任务（前端传 index 或者 taskId）
    this.app.post('/api/run-scheduled', async (req, res) => {
      try {
        const { index, taskId } = req.body || {};
        let task = null;
        if (typeof index === 'number') {
          if (!Array.isArray(this.config.scheduledTasks) || index < 0 || index >= this.config.scheduledTasks.length) {
            return res.status(400).json({ success: false, message: '无效的任务索引' });
          }
          task = this.config.scheduledTasks[index];
        } else if (taskId) {
          task = (this.config.scheduledTasks || []).find(t => t.id === taskId);
          if (!task) return res.status(404).json({ success: false, message: '未找到指定任务' });
        } else {
          return res.status(400).json({ success: false, message: '请提供 index 或 taskId' });
        }

        if (!task) return res.status(404).json({ success: false, message: '未找到任务' });
        if (task.disabled) return res.status(400).json({ success: false, message: '任务已禁用' });

        const queryId = await this.executeScheduledTask(task);
        if (!queryId) return res.status(500).json({ success: false, message: '任务触发失败' });

        res.json({ success: true, queryId });
      } catch (error) {
        logger.error('触发定时任务异常', error);
        res.status(500).json({ success: false, message: '触发定时任务失败', error: error.message });
      }
    });
  }

  setupWebSocket() {
    this.wss.on('connection', (ws) => {
      const clientId = uuidv4();
      this.connections.set(clientId, ws);
      logger.info(`新客户端连接: ${clientId}`);

      ws.on('message', (message) => {
        try {
          const data = JSON.parse(message);
          logger.info(`收到客户端消息: ${clientId}`, data);

          switch (data.type) {
            case 'subscribe':
              // 订阅查询状态
              if (data.queryId) {
                ws.querySubscriptions = ws.querySubscriptions || new Set();
                ws.querySubscriptions.add(data.queryId);
              }
              break;
            case 'unsubscribe':
              if (data.queryId && ws.querySubscriptions) {
                ws.querySubscriptions.delete(data.queryId);
              }
              break;
          }
        } catch (error) {
          logger.error('WebSocket消息处理失败', error);
        }
      });

      ws.on('close', () => {
        this.connections.delete(clientId);
        logger.info(`客户端断开连接: ${clientId}`);
      });

      ws.on('error', (error) => {
        logger.error(`WebSocket错误: ${clientId}`, error);
      });
    });
  }

  setupStaticFiles() {
    this.app.use(express.static(path.join(__dirname, 'public')));

    // 单页应用路由
    this.app.get('*', (req, res) => {
      res.sendFile(path.join(__dirname, 'public', 'index.html'));
    });
  }

  async executeQueriesAsync(queryId, databases) {
    const queryRecord = this.activeQueries.get(queryId);



    try {
      // queryRecord.results均设置为running
      await new Promise(resolve => setTimeout(resolve, 1000));
      this.broadcastQueryUpdate(queryId);
      // 等待1000ms
      await new Promise(resolve => setTimeout(resolve, 200));
      const promises = databases.map(async (db) => {
        const result = await this.executeSingleQuery(db);
        queryRecord.results[db.id] = result;
        this.broadcastQueryUpdate(queryId);
        return result;
      });

      await Promise.all(promises);

      queryRecord.status = 'completed';
      queryRecord.endTime = new Date();
      this.broadcastQueryUpdate(queryId);

      // 发送飞书通知
      if (this.config.feishuWebhook && this.config.feishuWebhook != '') {
        await this.sendFeishuNotification(queryRecord);
      }

    } catch (error) {
      queryRecord.status = 'failed';
      queryRecord.error = error.message;
      queryRecord.endTime = new Date();
      this.broadcastQueryUpdate(queryId);
      logger.error('查询执行失败', error);
    } finally {
      // 5分钟后清理完成的查询
      setTimeout(() => {
        this.activeQueries.delete(queryId);
      }, 300000);
    }
  }

  async executeSingleQuery(dbConfig) {
    const startTime = new Date();
    let connection;

    try {
      logger.info(`开始执行查询: ${dbConfig.name}`);

      connection = await mysql.createConnection({
        host: dbConfig.host,
        port: dbConfig.port,
        user: dbConfig.user,
        password: dbConfig.password,
        database: dbConfig.database,
        connectTimeout: 30000,
        timeout: this.config.queryTimeout
      });

      const [rows] = await connection.execute(this.config.sqlQuery);
      const endTime = new Date();

      logger.info(`查询完成: ${dbConfig.name}, 行数量: ${rows.length}`);

      return {
        status: 'success',
        database: dbConfig.name,
        databaseId: dbConfig.id,
        dbInfo: dbConfig,
        startTime: startTime.toISOString(),
        endTime: endTime.toISOString(),
        executionTime: endTime - startTime,
        results: rows,
        rowCount: rows.length
      };

    } catch (error) {
      const endTime = new Date();
      logger.error(`查询失败: ${dbConfig.name}`, error);

      return {
        status: 'failed',
        database: dbConfig.name,
        databaseId: dbConfig.id,
        dbInfo: dbConfig,
        startTime: startTime.toISOString(),
        endTime: endTime.toISOString(),
        executionTime: endTime - startTime,
        error: error.message
      };

    } finally {
      if (connection) {
        try {
          await connection.end();
        } catch (closeError) {
          logger.error(`连接关闭失败: ${dbConfig.name}`, closeError);
        }
      }
    }
  }

  broadcastQueryUpdate(queryId) {
    const query = this.activeQueries.get(queryId);
    if (!query) return;

    const message = JSON.stringify({
      type: 'queryUpdate',
      queryId,
      data: query
    });

    // 只发送给订阅了这个查询的客户端
    this.connections.forEach((ws, clientId) => {
      if (ws.readyState === WebSocket.OPEN && ws.querySubscriptions?.has(queryId)) {
        try {
          ws.send(message);
        } catch (error) {
          logger.error(`消息发送失败: ${clientId}`, error);
        }
      }
    });
  }

  async sendFeishuNotification(queryRecord, webhook = this.config.feishuWebhook, isScheduledTask = false, customJs = null) {
    try {
      console.log('发送飞书通知', queryRecord);
      var meesage = '';

      // 如果提供了自定义 JS，则在 vm 沙箱执行，允许用户返回字符串或对象 { content, webhook }
      if (customJs && typeof customJs === 'string' && customJs.trim().length > 0) {
        meesage += '自定义脚本执行结果：\n'
        try {
          const sandbox = { queryRecord, result: null, console };
          vm.createContext(sandbox);
          const wrapped = `result = (function(queryRecord){\n${customJs}\n})(queryRecord);`;
          vm.runInContext(wrapped, sandbox, { timeout: 2000 });
          const userResult = sandbox.result;
          if (typeof userResult === 'string') {
            meesage += userResult;
          } else if (userResult && typeof userResult === 'object') {
            if (userResult.webhook) webhook = userResult.webhook;
            if (userResult.content) meesage += userResult.content;
            else if (userResult.msg) meesage += userResult.msg;
            else meesage += JSON.stringify(userResult);
          }
        } catch (vmErr) {
          logger.error('执行自定义通知脚本失败', vmErr);
          // fallback to default message
          meesage += '执行自定义通知脚本失败';
        }

        meesage += '\n-----------------------\n'
      }

      // 默认消息构建（当自定义脚本没有产出内容时）

      meesage += `## 数据库查询结果\n`;
      meesage += `queryId >> ${queryRecord.id}  \n`;
      meesage += `| 数据库 | 行数 |  \n`;
      for (const dbId in queryRecord.results) {
        const db = queryRecord.results[dbId];
        if (db.status === 'success') {
          meesage += `| ${db.database} | ${db.rowCount} | \n`;
        }
        else if (db.status === 'running') {
          meesage += `| ${db.database} | 正在查询... | \n`;
        }
        else if (db.status === 'failed') {
          meesage += `| ${db.database} | 查询失败 | \n`;
        }
      }
      meesage += `\n`;
      meesage += `数据库具体数据——\n`;

      for (const dbId in queryRecord.results) {
        const db = queryRecord.results[dbId];
        meesage += `### ${db.dbInfo.host}\n`;
        if (db.status === 'success') {
          const keys = Object.keys(db.results[0] || {});
          meesage += `| ${keys.join(' | ')} |\n`;
          meesage += `| ${keys.map(() => '---').join(' | ')} |\n`;
          const displayRows = db.results.length > 15 ? db.results.slice(0, 15) : db.results;
          displayRows.forEach(row => {
            const values = keys.map(key => row[key]);
            meesage += `| ${values.join(' | ')} |\n`;
          });
          if (db.results.length > 15) {
            meesage += `| ... | ... |\n`;
          }
          meesage += `返回的行数：${db.rowCount}`;
          meesage += `查询用时：${db.executionTime} 毫秒\n`;
          meesage += `开始时间：${db.startTime}\n`;
          meesage += `结束时间：${db.endTime}\n`;
          meesage += `\n`;
        } else if (db.status === 'failed') {
          meesage += ` ❌❌ 查询失败 ❌❌ \n`;
        }
      }


      const content = {
        "msg_type": "interactive",
        "card": {
          "elements": [{
            "tag": "div",
            "text": {
              "content": meesage,
              "tag": "lark_md"
            }
          }]
        }
      };

      const response = await fetch(webhook, {
        method: 'POST',
        body: JSON.stringify(content),
        timeout: 30000
      });
      console.log('飞书通知发送结果', response);

      if (response.ok) {
        logger.info('飞书通知发送成功');
      } else {
        logger.error('飞书通知发送失败', await response.text());
      }

    } catch (error) {
      logger.error('飞书通知发送异常', error);
    }
  }

  async start(port) {
    // 等待初始化完成（配置加载、调度器启动等）
    if (this._initialized) await this._initialized;

    const listenPort = port || (this.config && this.config.serverPort) || 3000;
    this.server.listen(listenPort, () => {
      logger.info(`服务器启动成功，监听端口 ${listenPort}`);
      logger.info(`访问 http://localhost:${listenPort} 查看网页界面`);
    });

    this.server.on('error', (error) => {
      logger.error('服务器启动失败', error);
    });
  }
}

// 创建并启动应用
const bot = new DatabaseQueryBot();
bot.start();
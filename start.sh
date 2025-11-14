#!/bin/bash
# 多数据库查询机器人启动脚本

echo "====================================="
echo "    多数据库查询机器人 - Web版"
echo "====================================="
echo

# 检查Node.js是否安装
if ! command -v node &> /dev/null; then
    echo "❌ 错误：Node.js 未安装"
    echo "请先安装Node.js: https://nodejs.org/"
    exit 1
fi

# 检查npm是否安装
if ! command -v yarn &> /dev/null; then
    echo "❌ 错误：yarn 未安装"
    echo "请先安装yarn: https://yarnpkg.com/"
    exit 1
fi

# 检查是否在项目目录
if [ ! -f "package.json" ]; then
    echo "❌ 错误：未找到package.json文件"
    echo "请在项目根目录下运行此脚本"
    exit 1
fi

# 检查依赖包是否安装
echo "🔍 检查依赖包..."
if [ ! -d "node_modules" ]; then
    echo "📦 正在安装依赖包..."
    yarn install
    if [ $? -ne 0 ]; then
        echo "❌ 依赖包安装失败"
        exit 1
    fi
fi

# 检查配置文件是否存在
if [ ! -f "config.json" ]; then
    
    echo "❌ 未找到配置文件，请复制config_template.json为config.json并进行配置"
    exit 1
fi

# 检查是否存在combined.log和error.log日志文件
if [ ! -f "combined.log" ]; then
    touch combined.log
fi

if [ ! -f "error.log" ]; then
    touch error.log
fi

# 启动应用
echo "🚀 启动多数据库查询机器人..."
echo "====================================="
echo "访问地址: http://localhost:3000"
echo "日志文件: combined.log"
echo "错误日志: error.log"
echo "====================================="
echo

npm start
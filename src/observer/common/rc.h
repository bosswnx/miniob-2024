/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021/5/2.
//

#pragma once

/**
 * @brief 这个文件定义函数返回码/错误码(Return Code)
 * @enum RC
 */

#define DEFINE_RCS                                                      \
  DEFINE_RC(SUCCESS)                     /* 成功 */                   \
  DEFINE_RC(INVALID_ARGUMENT)            /* 无效参数 */             \
  DEFINE_RC(UNIMPLEMENTED)               /* 未实现 */                \
  DEFINE_RC(SQL_SYNTAX)                  /* SQL语法错误 */          \
  DEFINE_RC(INTERNAL)                    /* 内部错误 */             \
  DEFINE_RC(NOMEM)                       /* 内存不足 */             \
  DEFINE_RC(NOTFOUND)                    /* 未找到 */                \
  DEFINE_RC(EMPTY)                       /* 为空 */                   \
  DEFINE_RC(FULL)                        /* 已满 */                   \
  DEFINE_RC(EXIST)                       /* 已存在 */                \
  DEFINE_RC(NOT_EXIST)                   /* 不存在 */                \
  DEFINE_RC(BUFFERPOOL_OPEN)             /* 缓冲池已打开 */       \
  DEFINE_RC(BUFFERPOOL_NOBUF)            /* 缓冲池无缓冲区 */    \
  DEFINE_RC(BUFFERPOOL_INVALID_PAGE_NUM) /* 缓冲池无效页号 */    \
  DEFINE_RC(RECORD_OPENNED)              /* 记录已打开 */          \
  DEFINE_RC(RECORD_INVALID_RID)          /* 记录无效RID */          \
  DEFINE_RC(RECORD_INVALID_KEY)          /* 记录无效键 */          \
  DEFINE_RC(RECORD_DUPLICATE_KEY)        /* 记录重复键 */          \
  DEFINE_RC(RECORD_NOMEM)                /* 记录内存不足 */       \
  DEFINE_RC(RECORD_EOF)                  /* 记录结束 */             \
  DEFINE_RC(RECORD_NOT_EXIST)            /* 记录不存在 */          \
  DEFINE_RC(RECORD_INVISIBLE)            /* 记录不可见 */          \
  DEFINE_RC(SCHEMA_DB_EXIST)             /* 数据库模式已存在 */ \
  DEFINE_RC(SCHEMA_DB_NOT_EXIST)         /* 数据库模式不存在 */ \
  DEFINE_RC(SCHEMA_DB_NOT_OPENED)        /* 数据库模式未打开 */ \
  DEFINE_RC(SCHEMA_TABLE_NOT_EXIST)      /* 表模式不存在 */       \
  DEFINE_RC(SCHEMA_TABLE_EXIST)          /* 表模式已存在 */       \
  DEFINE_RC(SCHEMA_FIELD_NOT_EXIST)      /* 字段模式不存在 */    \
  DEFINE_RC(SCHEMA_FIELD_MISSING)        /* 字段模式缺失 */       \
  DEFINE_RC(SCHEMA_FIELD_TYPE_MISMATCH)  /* 字段类型不匹配 */    \
  DEFINE_RC(SCHEMA_INDEX_NAME_REPEAT)    /* 索引名称重复 */       \
  DEFINE_RC(IOERR_READ)                  /* 读IO错误 */              \
  DEFINE_RC(IOERR_WRITE)                 /* 写IO错误 */              \
  DEFINE_RC(IOERR_ACCESS)                /* 访问IO错误 */           \
  DEFINE_RC(IOERR_OPEN)                  /* 打开IO错误 */           \
  DEFINE_RC(IOERR_CLOSE)                 /* 关闭IO错误 */           \
  DEFINE_RC(IOERR_SEEK)                  /* 定位IO错误 */           \
  DEFINE_RC(IOERR_TOO_LONG)              /* IO操作过长 */           \
  DEFINE_RC(IOERR_SYNC)                  /* 同步IO错误 */           \
  DEFINE_RC(LOCKED_UNLOCK)               /* 解锁错误 */             \
  DEFINE_RC(LOCKED_NEED_WAIT)            /* 需要等待锁 */          \
  DEFINE_RC(LOCKED_CONCURRENCY_CONFLICT) /* 并发冲突锁 */          \
  DEFINE_RC(FILE_EXIST)                  /* 文件已存在 */          \
  DEFINE_RC(FILE_NOT_EXIST)              /* 文件不存在 */          \
  DEFINE_RC(FILE_NAME)                   /* 文件名错误 */          \
  DEFINE_RC(FILE_BOUND)                  /* 文件边界错误 */       \
  DEFINE_RC(FILE_CREATE)                 /* 文件创建错误 */       \
  DEFINE_RC(FILE_OPEN)                   /* 文件打开错误 */       \
  DEFINE_RC(FILE_NOT_OPENED)             /* 文件未打开 */          \
  DEFINE_RC(FILE_CLOSE)                  /* 文件关闭错误 */       \
  DEFINE_RC(FILE_REMOVE)                 /* 文件删除错误 */       \
  DEFINE_RC(VARIABLE_NOT_EXISTS)         /* 变量不存在 */          \
  DEFINE_RC(VARIABLE_NOT_VALID)          /* 变量无效 */             \
  DEFINE_RC(LOGBUF_FULL)                 /* 日志缓冲区已满 */    \
  DEFINE_RC(LOG_FILE_FULL)               /* 日志文件已满 */       \
  DEFINE_RC(LOG_ENTRY_INVALID)           /* 日志条目无效 */       \
  DEFINE_RC(UNSUPPORTED)                 /* 不支持的操作 */       \
  DEFINE_RC(VALUE_TYPE_MISMATCH)         /* 值类型不匹配 */

enum class RC
{
#define DEFINE_RC(name) name,
  DEFINE_RCS
#undef DEFINE_RC
};

extern const char *strrc(RC rc);

extern bool OB_SUCC(RC rc);
extern bool OB_FAIL(RC rc);

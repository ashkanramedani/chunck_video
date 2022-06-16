/*
 Navicat Premium Data Transfer

 Source Server         : fileManager
 Source Server Type    : SQLite
 Source Server Version : 3030001
 Source Schema         : main

 Target Server Type    : SQLite
 Target Server Version : 3030001
 File Encoding         : 65001

 Date: 16/06/2022 12:02:51
*/

PRAGMA foreign_keys = false;

-- ----------------------------
-- Table structure for files
-- ----------------------------
DROP TABLE IF EXISTS "files";
CREATE TABLE "files" (
  "file_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  "file_name" TEXT NOT NULL,
  "file_path" TEXT,
  "file_size" INTEGER,
  "file_format" TEXT,
  "part_number" INTEGER,
  "total_part" INTEGER,
  "parent_id" INTEGER
);

-- ----------------------------
-- Auto increment value for files
-- ----------------------------

PRAGMA foreign_keys = true;

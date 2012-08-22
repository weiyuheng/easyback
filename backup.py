#!/usr/bin/env python
# -*- coding: gbk -*-

'''MySQL备份程序

运行环境：
    CentOS
    Python2.6
安装方法：
    安装python
    # yum install python
    安装crontab
    # yum install crontabs
    检查cron服务是否启动启动
    # chkconfig 
    启动cron服务
    # service crond start
    安装备份程序
    # mv backup.py /usr/bin
    # chmod +x backup.py
    增加cron任务：每天0点5分执行
    # crontab -e
    # 5 0 * * * backup.py all
    # crontab -l 
    加载cron任务
    # service crond reload
注意事项：
    1. 修改配置信息时注意文件格式为UNIX、文件中缩进为4个空格，如果使用TAB则必须将TAB转为空格
    2. 默认备份路径为/backups, /backups/mysql, 运行前需先创建这两个路径，日志文件存放在/backups下，数据库备份存放在/backups/mysql下
    3. 确保运行用户具备备份路径的写入权限
    4. 删除历史文件时根据文件的ctime判断其是否过期，所以要避免对备份文件进行chmod、chown等会影响ctime的操作
'''


import os
import sys
import traceback 
import gzip
import logging
import re
import subprocess
import commands
import time
from datetime import timedelta, date
import smtplib
from email.mime.text import MIMEText

# 备份路径
backuppath = '/backups'
# 日期格式
timeformat = '%Y-%m-%d'
# 保留最新备份数
keepdays = 30

# 日志级别
loglevel = logging.INFO
# 日志格式
logformat = '%(asctime)s %(levelname)s: %(message)s'
# 日志文件路径
logfile = os.path.join(backuppath, 'backup.log')
# 加载日志配置信息
try:
   logging.basicConfig(filename=logfile, level=loglevel, format=logformat)
except IOError as e:
   logging.basicConfig(stream=sys.stdout, level=loglevel, format=logformat)
   logging.error('日志配置出错：%s', e)    

# 数据库服务器地址
dbhostname = 'localhost'
# 数据库用户名
dbuser = ''
# 数据库密码
dbpassword = ''
# 数据库备份文件存放目录
backuppath_db = os.path.join(backuppath, 'mysql')
# 需备份的数据库列表
backupdbs = ('', )

# 邮件发送服务器
mailhost = "smtp.xx.com"
# 发送用户的邮箱地址
mailuser= "wei_yh@xx.com"
# 发送用户的显示名称
mailuser_displayname = ""
# 发送用户的邮箱密码
mailpassword = ""
# 邮件接收人
mailto_list = ("", "")
# 出错发送标志：设为True时仅出错时发送
mail_when_error = True 

# 将文件压缩为gzip格式
def compress(filename, remove_origin=True):
    logging.info('开始压缩文件： %s', filename)
    f_in = open(filename, 'rb')
    f_out = gzip.open(filename + ".gz", 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()    
    logging.info('文件压缩完成： %s', filename + ".gz")
    
    logging.info('压缩完成后，删除原文件')
    if (remove_origin):
        os.remove(filename)

# 发送邮件        
def send_mail(mailto_list, subject, content):
    logging.info('开始发送邮件')
    me = "%s <%s>" % (mailuser_displayname, mailuser)
    msg = MIMEText(content, 'plain', 'gbk')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ",".join(mailto_list)
    try:
        s = smtplib.SMTP()
        s.connect(mailhost)
        s.login(mailuser, mailpassword)
        logging.debug('邮件内容：\n%s', msg)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
    except Exception as e:
        logging.error('邮件发送出错：%s', e)
        return

    logging.info('邮件发送完成')

# 备份数据库
def backup_db():
    logging.info('开始备份数据库')
    filestamp = time.strftime(timeformat)
    for database in backupdbs:
        logging.info('数据库：%s', database)
        filebasename = '%s.%s.sql' % (database, filestamp)
        filename = os.path.join(backuppath_db, filebasename)
        logging.info('备份文件：%s', filebasename)
        
        back_cmd = "mysqldump -u %s -p%s -h %s -e --opt -c %s > %s" % (dbuser, dbpassword, dbhostname, database, filename)
        logging.info('备份命令: %s', re.sub(r' -p.*? -h', ' -p***** -h', back_cmd))
        
        #pipe = subprocess.Popen(back_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        #pipe_out = pipe.stdout.read()
        #if pipe_out != None and pipe_out != '':
        #    logging.info('命令执行输出：%s', pipe_out)
            
        #pipe_err = pipe.stderr.read()
        #if  pipe_err != None and pipe_out != '':
        #    logging.error('命令执行出错：%s', pipe_err)
        #    raise Exception('命令执行出错：%s' % pipe_err) 
        
        (status, output) = commands.getstatusoutput(back_cmd)
        
        logging.debug('命令执行返回代码 %s', status)

        if status != 0:
            logging.error('命令执行出错：%s', output)
            raise Exception(output) 
            
        if os.path.exists(filename) == False:
            logging.error('备份命令已执行，但备份文件不存在')
            raise Exception('System Error, bakcup file is not exist') 
            
        if os.path.getsize(filename) == 0:
            logging.error('备份命令已执行，但备份文件为空')
            raise Exception('System Error, backup file size is 0') 
            
        
        logging.info('压缩备份文件')
        compress(filename)
        
    logging.info('数据库备份结束')

# 清理历史备份
def clean_db_backups():
    logging.info('开始清理数据库历史备份文件')
    min_keep_date = (date.today() + timedelta(days=-keepdays)).strftime(timeformat)
    logging.info('清理%s前（%s天前）的备份', min_keep_date, keepdays)
    files = os.listdir(backuppath_db)
    files.sort()
    for fliebasename in files:
        file = os.path.join(backuppath_db, fliebasename)
        logging.debug('文件：%s', file)
        stat_info = os.stat(file)
        st_ctime = time.localtime(stat_info.st_ctime)        
        stat_ctime = time.strftime(timeformat, st_ctime)
        logging.debug('文件日期：%s', stat_ctime)
        
        if stat_ctime < min_keep_date:
            if (st_ctime.tm_mday == 1):
                logging.debug('每月第一天的文件不清理：%s', file)
            else:            
                logging.warning('删除已过期的文件：%s', file)
                os.remove(file)
        else:
            logging.debug('文件未过期')
            
    logging.info('数据库历史备份文件清理结束')

    
if __name__ == "__main__":
    arg0 = sys.argv[0]
    usage = '''请输入正确的参数
    %s  db    :  备份数据库
    %s  clean :  清理数据库历史备份
    %s  all   :  备份数据库并清理历史备份
    ''' % (arg0, arg0, arg0)
    
    if len(sys.argv) != 2:
        print(usage)
    else:
        arg = sys.argv[1]
        result = ''
        try:
            if arg == 'db':
                backup_db()
            elif arg == 'clean':
                clean_db_backups()
            elif arg == 'all':
                backup_db()
                clean_db_backups()
            else:
                print(usage)
        # except Exception as e:
        except:
            # e = sys.exc_info()
            e = traceback.format_exc()
            logging.error('备份出错：%s', e)
            subject = '%s 数据库系统备份出错' % time.strftime(timeformat)
            content = '''%s：
            %s
            请查看备份日志文件%s，以获取详细错误信息！
            ''' % (subject, e, logfile)
            send_mail(mailto_list, subject, content)
        else:
            if mail_when_error == False:            
                subject = '%s 数据库系统备份完成' % time.strftime(timeformat)
                content = '''%s，请查看备份日志文件%s，以获取详细信息！
                ''' % (subject, logfile)
                send_mail(mailto_list, subject, content)
                
    


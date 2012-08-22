#!/usr/bin/env python
# -*- coding: gbk -*-

'''MySQL���ݳ���

���л�����
    CentOS
    Python2.6
��װ������
    ��װpython
    # yum install python
    ��װcrontab
    # yum install crontabs
    ���cron�����Ƿ���������
    # chkconfig 
    ����cron����
    # service crond start
    ��װ���ݳ���
    # mv backup.py /usr/bin
    # chmod +x backup.py
    ����cron����ÿ��0��5��ִ��
    # crontab -e
    # 5 0 * * * backup.py all
    # crontab -l 
    ����cron����
    # service crond reload
ע�����
    1. �޸�������Ϣʱע���ļ���ʽΪUNIX���ļ�������Ϊ4���ո����ʹ��TAB����뽫TABתΪ�ո�
    2. Ĭ�ϱ���·��Ϊ/backups, /backups/mysql, ����ǰ���ȴ���������·������־�ļ������/backups�£����ݿⱸ�ݴ����/backups/mysql��
    3. ȷ�������û��߱�����·����д��Ȩ��
    4. ɾ����ʷ�ļ�ʱ�����ļ���ctime�ж����Ƿ���ڣ�����Ҫ����Ա����ļ�����chmod��chown�Ȼ�Ӱ��ctime�Ĳ���
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

# ����·��
backuppath = '/backups'
# ���ڸ�ʽ
timeformat = '%Y-%m-%d'
# �������±�����
keepdays = 30

# ��־����
loglevel = logging.INFO
# ��־��ʽ
logformat = '%(asctime)s %(levelname)s: %(message)s'
# ��־�ļ�·��
logfile = os.path.join(backuppath, 'backup.log')
# ������־������Ϣ
try:
   logging.basicConfig(filename=logfile, level=loglevel, format=logformat)
except IOError as e:
   logging.basicConfig(stream=sys.stdout, level=loglevel, format=logformat)
   logging.error('��־���ó���%s', e)    

# ���ݿ��������ַ
dbhostname = 'localhost'
# ���ݿ��û���
dbuser = ''
# ���ݿ�����
dbpassword = ''
# ���ݿⱸ���ļ����Ŀ¼
backuppath_db = os.path.join(backuppath, 'mysql')
# �豸�ݵ����ݿ��б�
backupdbs = ('', )

# �ʼ����ͷ�����
mailhost = "smtp.xx.com"
# �����û��������ַ
mailuser= "wei_yh@xx.com"
# �����û�����ʾ����
mailuser_displayname = ""
# �����û�����������
mailpassword = ""
# �ʼ�������
mailto_list = ("", "")
# �����ͱ�־����ΪTrueʱ������ʱ����
mail_when_error = True 

# ���ļ�ѹ��Ϊgzip��ʽ
def compress(filename, remove_origin=True):
    logging.info('��ʼѹ���ļ��� %s', filename)
    f_in = open(filename, 'rb')
    f_out = gzip.open(filename + ".gz", 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()    
    logging.info('�ļ�ѹ����ɣ� %s', filename + ".gz")
    
    logging.info('ѹ����ɺ�ɾ��ԭ�ļ�')
    if (remove_origin):
        os.remove(filename)

# �����ʼ�        
def send_mail(mailto_list, subject, content):
    logging.info('��ʼ�����ʼ�')
    me = "%s <%s>" % (mailuser_displayname, mailuser)
    msg = MIMEText(content, 'plain', 'gbk')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ",".join(mailto_list)
    try:
        s = smtplib.SMTP()
        s.connect(mailhost)
        s.login(mailuser, mailpassword)
        logging.debug('�ʼ����ݣ�\n%s', msg)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
    except Exception as e:
        logging.error('�ʼ����ͳ���%s', e)
        return

    logging.info('�ʼ��������')

# �������ݿ�
def backup_db():
    logging.info('��ʼ�������ݿ�')
    filestamp = time.strftime(timeformat)
    for database in backupdbs:
        logging.info('���ݿ⣺%s', database)
        filebasename = '%s.%s.sql' % (database, filestamp)
        filename = os.path.join(backuppath_db, filebasename)
        logging.info('�����ļ���%s', filebasename)
        
        back_cmd = "mysqldump -u %s -p%s -h %s -e --opt -c %s > %s" % (dbuser, dbpassword, dbhostname, database, filename)
        logging.info('��������: %s', re.sub(r' -p.*? -h', ' -p***** -h', back_cmd))
        
        #pipe = subprocess.Popen(back_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        #pipe_out = pipe.stdout.read()
        #if pipe_out != None and pipe_out != '':
        #    logging.info('����ִ�������%s', pipe_out)
            
        #pipe_err = pipe.stderr.read()
        #if  pipe_err != None and pipe_out != '':
        #    logging.error('����ִ�г���%s', pipe_err)
        #    raise Exception('����ִ�г���%s' % pipe_err) 
        
        (status, output) = commands.getstatusoutput(back_cmd)
        
        logging.debug('����ִ�з��ش��� %s', status)

        if status != 0:
            logging.error('����ִ�г���%s', output)
            raise Exception(output) 
            
        if os.path.exists(filename) == False:
            logging.error('����������ִ�У��������ļ�������')
            raise Exception('System Error, bakcup file is not exist') 
            
        if os.path.getsize(filename) == 0:
            logging.error('����������ִ�У��������ļ�Ϊ��')
            raise Exception('System Error, backup file size is 0') 
            
        
        logging.info('ѹ�������ļ�')
        compress(filename)
        
    logging.info('���ݿⱸ�ݽ���')

# ������ʷ����
def clean_db_backups():
    logging.info('��ʼ�������ݿ���ʷ�����ļ�')
    min_keep_date = (date.today() + timedelta(days=-keepdays)).strftime(timeformat)
    logging.info('����%sǰ��%s��ǰ���ı���', min_keep_date, keepdays)
    files = os.listdir(backuppath_db)
    files.sort()
    for fliebasename in files:
        file = os.path.join(backuppath_db, fliebasename)
        logging.debug('�ļ���%s', file)
        stat_info = os.stat(file)
        st_ctime = time.localtime(stat_info.st_ctime)        
        stat_ctime = time.strftime(timeformat, st_ctime)
        logging.debug('�ļ����ڣ�%s', stat_ctime)
        
        if stat_ctime < min_keep_date:
            if (st_ctime.tm_mday == 1):
                logging.debug('ÿ�µ�һ����ļ�������%s', file)
            else:            
                logging.warning('ɾ���ѹ��ڵ��ļ���%s', file)
                os.remove(file)
        else:
            logging.debug('�ļ�δ����')
            
    logging.info('���ݿ���ʷ�����ļ��������')

    
if __name__ == "__main__":
    arg0 = sys.argv[0]
    usage = '''��������ȷ�Ĳ���
    %s  db    :  �������ݿ�
    %s  clean :  �������ݿ���ʷ����
    %s  all   :  �������ݿⲢ������ʷ����
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
            logging.error('���ݳ���%s', e)
            subject = '%s ���ݿ�ϵͳ���ݳ���' % time.strftime(timeformat)
            content = '''%s��
            %s
            ��鿴������־�ļ�%s���Ի�ȡ��ϸ������Ϣ��
            ''' % (subject, e, logfile)
            send_mail(mailto_list, subject, content)
        else:
            if mail_when_error == False:            
                subject = '%s ���ݿ�ϵͳ�������' % time.strftime(timeformat)
                content = '''%s����鿴������־�ļ�%s���Ի�ȡ��ϸ��Ϣ��
                ''' % (subject, logfile)
                send_mail(mailto_list, subject, content)
                
    


# -*- coding: utf-8 -*-
import os
import sys

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func,load_env_variables

load_env_variables()

if __name__ == "__main__":
    print('Hello')
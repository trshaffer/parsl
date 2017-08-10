import parsl
from parsl import *

parsl.set_stream_logger()

from parsl.execution_provider.provider_factory import ExecProviderFactory



def test_factory_1() :

    config = {  "site" : "midway_westmere",
                "execution" :
                {  "executor" : "ipp",
                   "provider" : "slurm",
                   "channel"  : "local",
                   "options" :
                  {"init_parallelism" : 2,
                   "max_parallelism" : 2,
                   "min_parallelism" : 0,
                   "tasks_per_node"  : 1,
                   "nodes_granularity" : 1,
                   "queue" : "westmere",
                   "walltime" : "00:25:00",
                   "account" : "pi-wilde",
                   "submit_script_dir" : ".scripts"
                  }
                }}

    epf = ExecProviderFactory()
    executor = epf.make(config)


def test_factory_2() :

    config = {  "site" : "ipp_local",
                "execution" :
                {  "executor" : "ipp",
                   "provider" : "local",
                   "channel"  : "None",
                   "options" :
                  {"init_parallelism" : 2,
                   "max_parallelism" : 2,
                   "min_parallelism" : 0,
                   "walltime" : "00:25:00",
                  }
                }}

    epf = ExecProviderFactory()
    executor = epf.make(config)




if __name__ == '__main__' :

    #test_factory_1()
    test_factory_2()

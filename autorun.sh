#!/bin/bash
#Lib path of libvmem
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
CURR=$(pwd)
export LD_LIBRARY_PATH=$CURR/third/pmdk/src/nondebug:$LD_LIBRARY_PATH

# for w in ycsba ycsbb ycsbc ycsbd ycsbe ycsbf ycsbg ycsbh ycsbj ycsbak ycsbal ycsbam PiBench1 PiBench2 PiBench3 PiBench4 PiBench5 PiBench6 PiBench7 PiBench8
# do
#     for t in 1 4 8 16 24 32
#     do  
#         for h in HALO CCEH DASH CLEVEL PCLHT VIPER SOFT
#         do
#             numactl -N 0 ./$h $w $t
#             rm /data/pmem0/HLSH -rf
#             echo "---------------------------\n"
#         done
#     done
# done
# for w in ycsb4 
# for w in PiBench4 
# for w in ycsbb 
# for t in 1 4 8 16 24 36
        # for h in HALO HLSH 
        # for h in HALO 
        # for h in HLSH 
        # for h in SOFT 
        # for h in DASH 
        # for h in CLEVEL 
# for w in ycsbb ycsbc ycsbd ycsbf ycsbg ycsbh PiBench1 PiBench2 PiBench4 PiBench5 PiBench6 PiBench8  
# for w in PiBench3 PiBench1 PiBench4 PiBench7 PiBench5 PiBench8 
for w in ycsbd 
do
    # for t in 1 4 8 16 24 36
    for t in 36
    do  
        # for h in HLSH HALO VIPER SOFT
        for h in HALO
        do
            # for v in 16 32 64 128 256
            for v in 8
            do
                # for k in 1 10 100 200 300 400
                for k in 100 200 300
                do
                    # if [ $w = PiBench3 ] || [ $w = PiBench7 ]
                    # then
                        # numactl -N 0 ./$h $w $t $k >>.txt
                        # numactl -N 0 ./$h $w $t 0 $k $v 
                    # else
                        # numactl -N 0 ./$h $w $t $k $k $v 
                    # fi
                    numactl -N 0 ./$h ycsb4 $t $k 0 $v
                    numactl -N 0 ./$h ycsb4 $t 0 0 $v
                    rm /data/pmem0/*.data -rf
                    rm /data/pmem0/hash/* -rf
                    rm /data/pmem0/vmem_test -rf
                    rm /data/pmem0/HLSH* -rf
                    rm /data/pmem0/slm -rf
                    echo "-------------------------\n"
                done
            done
        done
    done
done
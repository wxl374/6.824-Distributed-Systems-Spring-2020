# 6.824-Distributed-Systems-Spring-2020
&emsp;&emsp;这个仓库主要用来保存6.824的课后作业，共有4个lab，实现语言为Go。

## 6.824
&emsp;&emsp;MIT的《分布式系统》课程，包括经典论文的阅读和几个lab的实现。课程主页：https://pdos.csail.mit.edu/6.824/

**论文阅读列表**
- [x] [<span id="MapReduce">MapReduce: Simplified Data Processing on Large Clusters</span>](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- [ ] [The Google File System](https://pdos.csail.mit.edu/6.824/papers/gfs.pdf)
- [ ] [The Design of a Practical System for Fault-Tolerant Virtual Machines](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)

待续\.\.\.

## Lab 1
&emsp;&emsp;实现了简易版的Map Reduce（参看[论文](#MapReduce)第3页Figure 1: Execution overview）。包含Master和Worker两个进程，使用RPC通信，Master为Worker分配Map或Reduce任务，Worker调用用户提供的Map或Reduce程序执行任务。支持多个Worker同时运行，Worker故障或执行任务超时时，Master会将任务分配给其他Worker。暂未实现[论文](#MapReduce)3.6小结提到的Backup Tasks。

**代码框架**  
&emsp;&emsp;在课程提供的代码基础上，修改了mr目录下的master.go，worker.go和rpc.go三个文件。master.go和worker.go分别为Master和Worker程序的主体实现，rpc.go定义了Master和Worker交互所需的数据结构。  
&emsp;&emsp;Master启动时，创建一个Master实例，维护各任务的状态。Master为Worker提供了两个RPC句柄，**AssignTask**为Worker分配Map或Reduce任务；Worker执行完一个任务时，调用**GetResult**通知Master自己的执行结果。  
&emsp;&emsp;Worker启动时，同步地、重复地向Master获取任务，直至Master返回所有任务全部完成，Worker退出。

**运行环境**  
&emsp;&emsp;在ubuntu-20.04.1-desktop-amd64+Go 1.13下测试通过。  
> * 最早使用WSL，Worker会在job执行完成时core dump，但是[WSL不支持生成core文件](https://github.com/Microsoft/WSL/issues/1262)
> * 而后使用VirtualBox，程序运行较慢达到测试脚本的超时时间180s，无法完成job
> * 最终在2010年的老笔记本上安装了Ubuntu，脚本测试通过

**如何运行**
1. 在main目录下（以下操作均为在main目录下执行）编译word-count插件/动态库：
```
go build -buildmode=plugin ../mrapps/wc.go
```
2. 运行Master：
```
rm mr-out*
go run mrmaster.go pg-*.txt
```
3. 在一个或多个其他窗口运行Worker：
```
go run mrworker.go wc.so
```

**测试脚本**  
```
sh main/test-mr.sh
```

## Lab 2A
待续\.\.\.

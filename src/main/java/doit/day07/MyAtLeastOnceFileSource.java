package doit.day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

/**
 * 如果想要使用OperatorState，必须使用CheckpointedFunction
 */
public class MyAtLeastOnceFileSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private transient ListState<Long> offsetState;

    private boolean flag = true;
    private Long offset = 0L;

    private String path;

    public MyAtLeastOnceFileSource(String path) {
        this.path = path;
    }

    /**
     * 反序列化后，在Open方法之前执行
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //初始化或恢复OperatorState
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offset-state", Long.class);
        //使用FunctionInitializationContext取OperatorState
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()) {
            for (Long l : offsetState.get()) {
                offset = l;
            }
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        //可以从指定的偏移量读取数据
        RandomAccessFile accessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt", "r");
        accessFile.seek(offset); //设置从指定的位置读取数据

        while (flag) {

            String line = accessFile.readLine();
            //有新的文件写入
            if (line != null) {
               synchronized (ctx.getCheckpointLock()) {
                   //获取最新的偏移量
                   offset = accessFile.getFilePointer();
                   //输出数据
                   ctx.collect(indexOfThisSubtask + ".txt " + offset + " > " + line);
               }
            } else {
                Thread.sleep(1000);
            }
        }
    }

    /**
     * 每一次checkpoint时都会执行一次snapshotState方法
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        //将最新的状态添加进行
        offsetState.add(offset);
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

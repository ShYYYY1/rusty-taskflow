1.Task输入统一改成Arc元组，避免FromAnyVec中使用Arc::try_unwrap
2.FromAnyVec-> FromAnyVecDeque避免reverse，同样可以保序获取输入
3.框架层面强制要求Task输入只能是不可变引用，避免移动语义导致过程宏需要Arc::try_unwrap获取所有权但outputs一直持有导致必须进行copy，避免可变引用带来的并发问题以及从业务含义上的维护难度

仍然存在的成本
1.HashMap未特化(TaskId从0自增，有没有可能不用hashmap？不行的话可以考虑passthrough hash)
2.任务入参转换，目前的操作先VecDeque::pop_front()，再arc.downcast，操作成本包括pop_front以及downcast（先类型检查，再移动Arc句柄）；后续考虑改为Iter+Any::downcast_ref

在 Flow::run 里构造 VecDeque 后传给 invoke（src/tf/flow.rs:113, src/tf/flow.rs:123），再传给 from_any_vecdeque（src/tf/task.rs:78, src/tf/task.rs:79），这两步主要是移动容器元数据，不是复制底层元素缓冲区。
VecDeque move 通常只是几个 machine word（指针/len/cap/head），开销很小；编译器在 --release 下大概率会内联并做寄存器传递/消除临时。
所以这两次 move 不是当前主要瓶颈。更重的通常是：
依赖收集阶段的 outputs.get(...).cloned()（Arc clone + hash 查找）src/tf/flow.rs:115-117
Arc<dyn Any> 的 downcast 路径 src/tf/task.rs:29-34
你可以优化，但建议优先级这样排：

先不纠结两次 move，先 profile（perf/flamegraph）确认热点。
若要微优化 this path，优先改 API 成：
from_any_vecdeque(&mut VecDeque<...>)，减少一次按值传递；
或直接改为 Vec + into_iter().next()（元数据更轻，cache 行为通常更好）。
更大的收益点一般在“减少 Arc clone / HashMap 查找次数 / downcast 次数”。
一句话结论：
编译期可能会把这两次转移优化得很轻，它们大概率不是主要开销；但这条路径确实可优化，只是优先级应低于 Arc+downcast+map 查找。
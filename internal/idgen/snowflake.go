// 已就位（AI 生成）：位布局常量 + 构造函数是样板；核心算法 NextID 留给 p2。
package idgen

import (
	"fmt"
	"sync"
	"time"
)

const (
	epochMilli int64 = 1735689600000 // 2025-01-01T00:00:00Z，自定义纪元——缩短时间戳位数的可用年限内够用
	nodeBits   uint  = 10
	seqBits    uint  = 12
	maxNodeID  int64 = -1 ^ (-1 << nodeBits) // 1023
	maxSeq     int64 = -1 ^ (-1 << seqBits)  // 4095
	nodeShift        = seqBits
	timeShift        = seqBits + nodeBits
)

// Node 是一台机器/一个进程的雪花 ID 生成器。nowFunc 默认是 time.Now 的毫秒版，
// 测试时可以换成可控的假时钟来稳定复现"时钟回拨"这条边界分支。
type Node struct {
	mu      sync.Mutex
	nodeID  int64
	lastTs  int64
	seq     int64
	nowFunc func() int64
}

// NewNode 校验 nodeID 落在 10 bit 能表示的范围内（0~1023），并给 nowFunc 装上默认实现。
func NewNode(nodeID int64) (*Node, error) {
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("idgen: nodeID %d out of range [0,%d]", nodeID, maxNodeID)
	}
	return &Node{
		nodeID:  nodeID,
		lastTs:  -1,
		nowFunc: func() int64 { return time.Now().UnixMilli() },
	}, nil
}

// NextID 是 p2 的核心：标准雪花算法——41 位毫秒时间戳 + 10 位机器号 + 12 位序列号，
// 拼成一个全局单调递增（时钟不回拨的前提下）、跨进程不冲突的 int64 订单号。
func (n *Node) NextID() (int64, error) {
	panic("TODO: phase p2 - 拼出 时间戳(41)+节点号(10)+序列号(12) 三段式 ID，并处理时钟回拨")
}

// AI 将在 p2 学习时分切片实现（下面是思路与顺序，不是终稿代码）：
// 1. n.mu.Lock(); defer n.mu.Unlock() —— 同一个 Node 会被多个 goroutine 共用，seq/lastTs 是共享可变状态
// 2. now := n.nowFunc()
// 3. if now < n.lastTs { return 0, fmt.Errorf("idgen: clock moved backwards, refusing to generate id for %dms", n.lastTs-now) }
//    —— 时钟回拨怎么办：本课选"直接拒绝"而不是"阻塞等到时钟追上"——拒绝更简单也更适合面试讲清楚
//    权衡（可用性 vs 正确性），阻塞方案的风险（等多久？NTP 大幅跳变时阻塞可能是几秒到几分钟）留给 quiz 展开。
// 4. if now == n.lastTs {
//      n.seq = (n.seq + 1) & maxSeq          // 同一毫秒内序列号自增，用 & maxSeq 让它超过 4095 后回卷到 0
//      if n.seq == 0 {                        // 回卷到 0 说明这一毫秒 4096 个号已经发完
//        for now <= n.lastTs { now = n.nowFunc() }  // 忙等到下一毫秒，避免同毫秒内 ID 重复
//      }
//    } else {
//      n.seq = 0                              // 换了一个新的毫秒，序列号从 0 重新计
//    }
// 5. n.lastTs = now
// 6. id := ((now - epochMilli) << timeShift) | (n.nodeID << nodeShift) | n.seq
// 7. return id, nil

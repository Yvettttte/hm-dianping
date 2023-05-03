package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("Error",e);
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();//因为此时开创了新线程，该线程是取不到userholder的
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //事务提交之后再释放锁
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock();
        if(!isLock){
            log.error("不允许重复下单");
            return;
        }

        try {
            return proxy.createVoucherOrder(voucherOrder);
        } finally {
            simpleRedisLock.unlock();
        }
    }

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;
    public Result seckillVoucherWithLua(Long VoucherId){
        Long id = UserHolder.getUser().getId();
        Long result = 0L; //伪装成执行lua脚本后得到的结果。
        int r = result.intValue();
        if(r != 0){
            return Result.fail(r == 1? "Out of Stock" : "Have made same order");
        }
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(VoucherId);
        voucherOrder.setUserId(id);
        orderTasks.add(voucherOrder);
        //获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher.getBeginTime().isAfter(LocalDateTime.now()) ||voucher.getEndTime().isBefore(LocalDateTime.now()) ) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getStock() < 1) {
            return Result.fail("Out of stock!");
        }

        Long userId = UserHolder.getUser().getId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //事务提交之后再释放锁
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock();
        if(!isLock){
            return Result.fail("一个人只允许下一单");
        }

        try {
            IVoucherOrderService o = (IVoucherOrderService) AopContext.currentProxy();
            return o.createVoucherOrder(voucherId);
        } finally {
            simpleRedisLock.unlock();
        }


    }
    @Transactional
    public Result createVoucherOrder(Long voucherId){

        //One person one order
        Long userId = UserHolder.getUser().getId();
        //如果在以下的代码块加锁，那么释放锁的操作将会早于事务提交的操作，因此依然可能存在该用户在未提交事物的情况下锁被另一线程抢到，同一用户下单两次的情况
        //因此应该加在方法上，但由于加锁的粒度只需要到用户即可，所以在之前的方法中加锁。

            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                return Result.fail("该用户已购买过");
            }

            boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherId).gt("stock", 0).update();
            if (!success) return Result.fail("Out of stock!");


            VoucherOrder voucherOrder = new VoucherOrder();
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(UserHolder.getUser().getId());
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            return Result.ok(orderId);

    }
}

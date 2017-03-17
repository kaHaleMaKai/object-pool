package com.github.kahalemakai.whirlpool;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class MutatorFactory implements PooledObjectFactory<Mutator<Long>> {
    @Override
    public PooledObject<Mutator<Long>> makeObject() throws Exception {
        return new DefaultPooledObject<>(new Mutator<>(Thread.currentThread().getId()));
    }

    @Override
    public void destroyObject(PooledObject<Mutator<Long>> p) throws Exception {
        p.getObject().set(-1L);
    }

    @Override
    public boolean validateObject(PooledObject<Mutator<Long>> p) {
        return true;
//        return (p.getObject().blockAndGet() + System.currentTimeMillis()) % 5 != 0;
    }

    @Override
    public void activateObject(PooledObject<Mutator<Long>> p) throws Exception {
        p.getObject().set(Thread.currentThread().getId());
    }

    @Override
    public void passivateObject(PooledObject<Mutator<Long>> p) throws Exception {
        p.getObject().set(0L);
    }
}

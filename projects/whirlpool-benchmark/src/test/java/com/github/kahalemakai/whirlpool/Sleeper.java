package com.github.kahalemakai.whirlpool;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

class Sleeper implements Benchmarkable {
    private int i;

    public Sleeper() {
        i = 1;
        Benchmarkable.sleep(500);
    }

    public void close() {
        i = 0;
        Benchmarkable.sleep(250);
    }

    public boolean validate() {
        Benchmarkable.sleep(30);
        return true;
    }

    public void prepare() {
        Benchmarkable.sleep(10);
    }

    public void reset() {
        Benchmarkable.sleep(10);
    }

    public static class SleeperFactory implements PooledObjectFactory<Sleeper> {
        @Override
        public PooledObject<Sleeper> makeObject() throws Exception {
            return new DefaultPooledObject<>(new Sleeper());
        }

        @Override
        public void destroyObject(PooledObject<Sleeper> p) throws Exception {
            p.getObject().close();
        }

        @Override
        public boolean validateObject(PooledObject<Sleeper> p) {
            return p.getObject().validate();
        }

        @Override
        public void activateObject(PooledObject<Sleeper> p) throws Exception {
            p.getObject().prepare();
        }

        @Override
        public void passivateObject(PooledObject<Sleeper> p) throws Exception {
            p.getObject().reset();
        }
    }
}

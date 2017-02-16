package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class HermesDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();


//      defineSimpleTableProviderComponents(all, "hermes", com.ctrip.hermes.broker.dal.hermes._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.hermes.broker.dal.hermes._INDEX.getDaoClasses());

      return all;
   }
}

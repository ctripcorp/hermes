package com.ctrip.hermes.metaservice.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class FxhermesmetadbDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();


      defineSimpleTableProviderComponents(all, "fxhermesmetadb", com.ctrip.hermes.metaservice.model._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.hermes.metaservice.model._INDEX.getDaoClasses());

      return all;
   }
}

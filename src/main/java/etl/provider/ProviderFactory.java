package etl.provider;

import etl.provider.impl.CassandraProvider;
import etl.provider.impl.YamlProvider;

public final class ProviderFactory {

    private ProviderFactory() {
    }

    public static Provider of(String provider) {
        if ("cassandra".equals(provider)) {
            return new CassandraProvider();
        }
        return new YamlProvider();
    }

}

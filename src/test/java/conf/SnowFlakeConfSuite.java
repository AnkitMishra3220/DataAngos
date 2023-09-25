package conf;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.conf.SnowFlakeConfig;
import com.da.digital.conf.SnowFlakeDataSourceConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {SnowFlakeConfig.class, AppConfig.class,SnowFlakeDataSourceConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class SnowFlakeConfSuite {

    @Autowired
    private SnowFlakeConfig SnowFlakeConfig;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void testSnowFlakeConf(){
        Assert.assertEquals("S_ETL_BDP_DEV",SnowFlakeConfig.getUserName());

        Assert.assertEquals(jdbcTemplate.queryForList("SELECT count(*) FROM TEST1", String.class).size(),1);

    }
}

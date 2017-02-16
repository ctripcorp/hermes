package feature;

import org.junit.runner.RunWith;
import org.unidal.lookup.ComponentTestCase;

import cucumber.api.CucumberOptions;
import cucumber.api.SnippetType;
import cucumber.api.junit.Cucumber;

@RunWith(Cucumber.class)
@CucumberOptions(monochrome = true, snippets = SnippetType.CAMELCASE)
public class CucumberTest extends ComponentTestCase {

}

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using VirtuosPlayFab.Config;
using VirtuosPlayFab.Service.LeaderboardService;
using VirtuosPlayFab.Service.RedisService;

namespace VirtuosPlayFab
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Add framework services.
            services.AddMvc();
            //services.AddSingleton<IServiceStackRedisServiceProvider>(x => new ServiceStackRedisServiceProvider(new RedisSettings()
            //{
            //    Host = Configuration["AppSettings:RedisConfig:Host"],
            //    Port = int.Parse(Configuration["AppSettings:RedisConfig:Port"])
            //}));
            //
            //
            //use IStackExchangeRedisServiceProvider Instead
            services.AddSingleton<IStackExchangeRedisServiceProvider>(x => new StackExchangeRedisServiceProvider(new RedisSettings()
            {
                Host = Configuration["AppSettings:RedisConfig:Host"],
                Port = int.Parse(Configuration["AppSettings:RedisConfig:Port"])
            }));
            services.Configure<Appsettings>(Configuration.GetSection("AppSettings"));
            services.AddTransient<ILeaderboardService, StackExchangeLeaderboardService>();
            services.AddSingleton<IConfiguration>(Configuration);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();
            app.UseMvc();                          
        }
    }
}

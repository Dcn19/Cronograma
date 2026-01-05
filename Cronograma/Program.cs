using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// ===== CONFIG DO HOST / URL =====
builder.WebHost.UseUrls("http://0.0.0.0:5000");

// ===== SERVIÇOS =====
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// ===== CORS =====
const string CorsPolicyName = "AllowCMESFront";

builder.Services.AddCors(options =>
{
    options.AddPolicy(CorsPolicyName, policy =>
    {
        policy
            .WithOrigins(
                //"http://192.168.100.108:9300"
                "http://192.168.100.121:5174"
            // se você abrir o front por localhost na sua máquina também:
            // "http://localhost:9300"
            )
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// ===== PIPELINE =====
app.UseDefaultFiles();
app.UseStaticFiles();

app.UseRouting();

// ✅ CORS tem que vir depois de UseRouting e antes do MapControllers
app.UseCors(CorsPolicyName);

app.UseAuthorization();

app.MapControllers();

app.Run();

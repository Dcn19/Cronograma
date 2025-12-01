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

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowCMESFront", policy =>
    {
        policy
            .WithOrigins("http://192.168.100.121:5174") // ajuste conforme a porta do Vite
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

app.UseCors("AllowCMESFront");

app.UseAuthorization();

app.MapControllers();

app.Run();

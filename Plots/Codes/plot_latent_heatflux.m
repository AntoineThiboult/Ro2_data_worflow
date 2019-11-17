ccc

dataPath = 'C:\Users\anthi182\Desktop\Micromet_data\Merged_csv';

foret_est = readtable(fullfile(dataPath,'Foret_est_merged_data.csv'));
foret_ouest = readtable(fullfile(dataPath,'Foret_ouest_merged_data.csv'));
berge = readtable(fullfile(dataPath,'Berge_merged_data.csv'));
% reservoir = readtable(fullfile(dataPath,'Reservoir_merged_data.csv'));
dateRef = datetime(foret_est.date_index);

% Despiking
foret_est.LE(foret_est.LE > 250 | foret_est.LE<-50) = NaN;
foret_ouest.LE(foret_ouest.LE > 250 | foret_ouest.LE<-50) = NaN;
berge.LE(berge.LE > 250 | berge.LE<-50) = NaN;
% reservoir.LE(reservoir.LE > 250 | reservoir.LE<-50) = NaN;
reservoir.LE=berge.LE;

% Conversion coefficient
convCoeff = 86.4/2264;

% LE
figure('Position',[-1920, -175, 1920, 964])
subplot(3,1,1)
hp1 = plot(dateRef, foret_est.LE*convCoeff); hn
hp2 = plot(dateRef, foret_ouest.LE*convCoeff);
LEsmoothLand = smooth(nanmean(cat(2,foret_est.LE,foret_ouest.LE),2),500) * convCoeff;
idLEsmoothLand = any(~isnan([foret_est.LE, foret_ouest.LE]),2);
hp3 = plot(dateRef(idLEsmoothLand), LEsmoothLand(idLEsmoothLand));
set(hp1, 'Color',[51 255 51]./255, 'LineStyle',':')
set(hp2, 'Color',[51 255 51]./255, 'LineStyle','--')
set(hp3, 'Color',[0  153 0]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('LE [mm/jour]')
grid on
title('Foret')

subplot(3,1,2)
hp1 = plot(dateRef, berge.LE*convCoeff); hn
hp2 = plot(dateRef, reservoir.LE*convCoeff);
LEsmoothWater = smooth(nanmean(cat(2,berge.LE,reservoir.LE),2),500) * convCoeff;
idLEsmoothWater = any(~isnan([berge.LE, reservoir.LE]),2);
hp3 = plot(dateRef(idLEsmoothWater), LEsmoothWater(idLEsmoothWater));
set(hp1, 'Color',[102 178 255]./255, 'LineStyle',':')
set(hp2, 'Color',[102 178 255]./255, 'LineStyle','--')
set(hp3, 'Color',[0   0   205]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('LE [mm/jour]')
grid on
title('Reservoir')

subplot(3,1,3)
diffLE = (nanmean(cat(2,berge.LE,reservoir.LE),2) - nanmean(cat(2,foret_est.LE,foret_ouest.LE),2)) * convCoeff;
diffLEsmooth = smooth(diffLE,500);
iddiffLEsmooth = ~isnan(diffLE);
hp1 = plot(dateRef, diffLE); hn
hp2 = plot(dateRef(iddiffLEsmooth), diffLEsmooth(iddiffLEsmooth));
set(hp1, 'Color',[150 150 150]./255, 'LineStyle',':')
set(hp2, 'Color',[0   0   0]./255, 'LineWidth',2)
xlim([min(dateRef), max(dateRef)])
ylabel('LE [mm/jour]')
grid on
title('Différence')

exportPdfCrop(gcf, '../Plots/LE.pdf')